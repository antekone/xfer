//! Real SFTP protocol integration, with a fake SSH transport to exercise askpass
//! without requiring an sshd account/password or modifying ~/.ssh/known_hosts.
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Daemon {
    child: Child,
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    root: tempfile::TempDir,
}
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Daemon {
    fn start() -> Option<Self> {
        let server = [
            "/usr/lib/ssh/sftp-server",
            "/usr/lib/openssh/sftp-server",
            "/usr/libexec/sftp-server",
        ]
        .into_iter()
        .find(|path| Path::new(path).exists());
        let Some(server) = server else {
            eprintln!("skipping installed-SFTP-server integration test: sftp-server not found");
            return None;
        };
        let root = tempfile::tempdir().unwrap();
        let remote = root.path().join("remote");
        fs::create_dir_all(remote.join("photos")).unwrap();
        fs::write(remote.join("hello world.txt"), b"hello remote world").unwrap();
        fs::set_permissions(
            remote.join("hello world.txt"),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        fs::File::open(remote.join("hello world.txt"))
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(1234567890)),
            )
            .unwrap();
        fs::write(remote.join("photos/image.txt"), b"image").unwrap();
        fs::write(
            remote.join("large.bin"),
            (0..200_000).map(|n| (n % 251) as u8).collect::<Vec<_>>(),
        )
        .unwrap();
        std::os::unix::fs::symlink("photos", remote.join("linked photos")).unwrap();
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let script = format!(
            r##"#!/bin/sh
# No credentials in process arguments or environment.
case "$*" in *test-secret*) exit 91;; esac
if env | grep -q test-secret; then exit 92; fi
printf '%s\n' "$$" >> "$TEST_SSH_PIDS"
case "$*" in
    *silent.invalid*) exit 42;;
    *stderr.invalid*) echo 'subsystem request failed on channel 0' >&2; exit 255;;
    *noexpand.invalid*) exec {server} -P expand-path -d "$TEST_REMOTE_ROOT";;
esac
if [ "$SSH_ASKPASS_REQUIRE" = never ]; then
    exec {server} -d "$TEST_REMOTE_ROOT"
fi
printf '%s\n' "$XFER_SSH_ASKPASS_SOCKET" >> "$TEST_ASKPASS_PATHS"
case "$*" in *stall.invalid*) exec sleep 30;; esac
answer=$("$SSH_ASKPASS" "test user's password:") || exit 93
if [ "$answer" != 'test-secret' ]; then
    echo 'Permission denied (password).' >&2
    exit 94
fi
unset answer
exec {server} -d "$TEST_REMOTE_ROOT"
"##
        );
        let ssh = bin.join("ssh");
        fs::write(&ssh, script).unwrap();
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700)).unwrap();
        let config = root.path().join("config/xfer");
        fs::create_dir_all(&config).unwrap();
        fs::write(
            config.join("xfer.conf"),
            "ui:history-file: session\nsftp:connect-timeout: 5\n",
        )
        .unwrap();
        fs::write(config.join("bookmarks.toml"), "[test]\ntype = 'sftp'\nurl = 'sftp://user@example.invalid'\npassword_interactive = true\n[stall]\ntype = 'sftp'\nurl = 'sftp://user@stall.invalid'\npassword_interactive = true\n").unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_xfer"))
            .arg("--daemon")
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("HOME", root.path().join("home"))
            .env("XDG_CONFIG_HOME", root.path().join("config"))
            .env("XDG_RUNTIME_DIR", root.path().join("run"))
            .env_remove("XFER_CONFIG")
            .env_remove(xfer::ssh_askpass::SOCKET_ENV)
            .env("TEST_REMOTE_ROOT", &remote)
            .env("TEST_ASKPASS_PATHS", root.path().join("askpass-paths"))
            .env("TEST_SSH_PIDS", root.path().join("ssh-pids"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let socket = root
            .path()
            .join(format!("run/xfer/instances/{}.sock", child.id()));
        let deadline = Instant::now() + Duration::from_secs(10);
        let stream = loop {
            if let Ok(stream) = UnixStream::connect(&socket) {
                break stream;
            }
            assert!(Instant::now() < deadline, "daemon socket did not appear");
            std::thread::sleep(Duration::from_millis(20));
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut daemon = Self {
            child,
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
            root,
        };
        daemon.stream.write_all(b"ATTACH\n").unwrap();
        assert_eq!(daemon.line(), "ATTACHED");
        Some(daemon)
    }
    fn line(&mut self) -> String {
        let mut line = String::new();
        assert_ne!(
            self.reader.read_line(&mut line).unwrap(),
            0,
            "daemon disconnected"
        );
        line.trim_end().to_owned()
    }
    fn request(&mut self, request: &str) -> Vec<String> {
        writeln!(self.stream, "{request}").unwrap();
        let mut lines = Vec::new();
        loop {
            let line = self.line();
            if line == "DONE" {
                break;
            }
            lines.push(line.strip_prefix("OUTPUT ").unwrap().to_owned());
        }
        lines
    }
    fn command(&mut self, command: &str) -> Vec<String> {
        self.request(&format!("COMMAND {command}"))
    }
    fn finish(&mut self, response: Vec<String>, succeeds: bool) -> Vec<String> {
        let id = response[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let status = self.command(&format!("status {id}"));
            assert!(!status[0].contains("test-secret"));
            if status[0].contains("Completed") || status[0].contains("Failed") {
                assert_eq!(status[0].contains("Completed"), succeeds, "{status:?}");
                break;
            }
            assert!(Instant::now() < deadline, "job did not finish: {status:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
        self.command(&format!("output {id}"))
    }
    fn connect(&mut self, password: &str, succeeds: bool) {
        let payload = serde_json::json!({"target": "test", "password": password});
        let response = self.request(&format!("CONNECT_PASSWORD {payload}"));
        self.finish(response, succeeds);
    }
    fn reattach(&mut self) {
        let socket = self
            .root
            .path()
            .join(format!("run/xfer/instances/{}.sock", self.child.id()));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut stream = UnixStream::connect(&socket).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream.write_all(b"ATTACH\n").unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut response = String::new();
            reader.read_line(&mut response).unwrap();
            if response.trim() == "ATTACHED" {
                self.stream = stream;
                self.reader = reader;
                return;
            }
            assert!(Instant::now() < deadline, "cannot reattach: {response}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn remote(&self) -> PathBuf {
        self.root.path().join("remote")
    }
}

#[test]
fn interactive_password_sftp_navigation_listing_completion_and_resumed_download() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    assert_eq!(
        daemon.command("open test"),
        vec!["PASSWORD_REQUIRED \"test\""]
    );
    assert!(daemon.command("jobs").is_empty());
    daemon.connect("test-secret", true);
    let home = format!("{}/", daemon.remote().display());
    assert!(daemon.command("status").contains(&format!("CWD: {home}")));
    let response = daemon.command("ls");
    let listing = daemon.finish(response, true).join("\n");
    assert!(listing.contains("hello world.txt"));
    assert!(listing.contains("photos/"));
    assert!(listing.contains("linked photos/"));
    assert!(!listing.contains("test-secret"));
    let file_line = listing
        .lines()
        .find(|line| line.ends_with("hello world.txt"))
        .unwrap();
    let fields = file_line.split_whitespace().collect::<Vec<_>>();
    let metadata = fs::metadata(daemon.remote().join("hello world.txt")).unwrap();
    assert_eq!(fields[0], "-rw-r-----");
    assert_eq!(fields[1], metadata.uid().to_string());
    assert_eq!(fields[2], metadata.gid().to_string());
    assert_eq!(&fields[3..7], &["[2009-02-13", "23:31:30", "UTC]", "18"]);
    let link_line = listing
        .lines()
        .find(|line| line.ends_with("linked photos/"))
        .unwrap();
    assert!(link_line.starts_with("lrwxrwxrwx"), "{link_line}");
    let large_line = listing
        .lines()
        .find(|line| line.ends_with("large.bin"))
        .unwrap();
    assert_eq!(large_line.split_whitespace().nth(6), Some("200000"));

    daemon.stream.write_all(b"COMPLETE_CD \"pho\"\n").unwrap();
    let line = daemon.line();
    let completion: xfer::cache::PathCompletion =
        serde_json::from_str(line.strip_prefix("COMPLETION ").unwrap()).unwrap();
    assert_eq!(completion.candidates, vec!["photos/"]);
    let response = daemon.command("cd 'linked photos'");
    daemon.finish(response, true);
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {home}photos/")));
    let response = daemon.command("cd missing");
    daemon.finish(response, false);
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {home}photos/")));
    let response = daemon.command("cd");
    daemon.finish(response, true);
    assert!(daemon.command("status").contains(&format!("CWD: {home}")));

    let downloads = daemon.root.path().join("downloads");
    fs::create_dir(&downloads).unwrap();
    writeln!(
        daemon.stream,
        "LOCAL_CWD {}",
        serde_json::to_string(&downloads).unwrap()
    )
    .unwrap();
    assert_eq!(daemon.line(), "LOCAL_CWD_OK");
    fs::write(downloads.join("hello world.txt"), b"hello ").unwrap();
    let response = daemon.command("get -c 'hello world.txt'");
    daemon.finish(response, true);
    assert_eq!(
        fs::read(downloads.join("hello world.txt")).unwrap(),
        b"hello remote world"
    );
    let response = daemon.command("get large.bin");
    daemon.finish(response, true);
    assert_eq!(
        fs::read(downloads.join("large.bin")).unwrap(),
        fs::read(daemon.remote().join("large.bin")).unwrap()
    );
    // A failed password does not replace the existing active connection.
    daemon.connect("wrong password", false);
    assert!(daemon.command("status").contains(&format!("CWD: {home}")));
    // Cancellation drops both the in-flight SSH process and its password helper.
    let payload = serde_json::json!({"target": "stall", "password": "test-secret"});
    let response = daemon.request(&format!("CONNECT_PASSWORD {payload}"));
    let id = response[0].strip_prefix("JOB ").unwrap().to_owned();
    std::thread::sleep(Duration::from_millis(50));
    daemon.command(&format!("cancel {id}"));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = daemon.command(&format!("status {id}"));
        if status[0].contains("Cancelled") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancellation did not finish: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let paths = fs::read_to_string(daemon.root.path().join("askpass-paths")).unwrap();
    for path in paths.lines() {
        assert!(
            !Path::new(path).parent().unwrap().exists(),
            "askpass helper leaked"
        );
    }
    let response = daemon.command("close");
    daemon.finish(response, true);
    assert!(daemon
        .command("status")
        .contains(&"Backend: disconnected".into()));
    // Direct URLs without the bookmark opt-in never prompt (agent/key mode).
    let response = daemon.command("open sftp://user@agent.invalid");
    daemon.finish(response, true);
}

#[test]
fn connection_failures_include_phase_cause_ssh_status_and_stderr() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let response = daemon.command("open sftp://user@silent.invalid");
    let error = daemon.finish(response, false).join("\n");
    assert!(error.contains("user@silent.invalid"), "{error}");
    assert!(error.contains("start SFTP subsystem:"), "{error}");
    assert!(error.contains("SSH exit status: 42"), "{error}");

    let response = daemon.command("open sftp://user@stderr.invalid");
    let error = daemon.finish(response, false).join("\n");
    assert!(error.contains("start SFTP subsystem:"), "{error}");
    assert!(error.contains("SSH exit status: 255"), "{error}");
    assert!(
        error.contains("SSH stderr: subsystem request failed on channel 0"),
        "{error}"
    );

    let url = format!(
        "sftp://user@agent.invalid{}/missing-directory",
        daemon.remote().display()
    );
    let response = daemon.command(&format!("open {url}"));
    let error = daemon.finish(response, false).join("\n");
    assert!(error.contains("initial remote directory"), "{error}");
    assert!(error.contains("missing-directory"), "{error}");
    assert!(error.contains("NoSuchFile"), "{error}");
}

#[test]
fn tilde_urls_expand_remotely_and_fall_back_without_the_extension() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let response = daemon.command("open sftp://user@agent.invalid/~");
    daemon.finish(response, true);
    let home = daemon
        .command("status")
        .into_iter()
        .find(|line| line.starts_with("CWD: "))
        .unwrap();
    assert_ne!(home, "CWD: /~/");
    assert!(home.starts_with("CWD: /"));
    let response = daemon.command("open sftp://user@agent.invalid/~/");
    daemon.finish(response, true);
    assert!(daemon.command("status").contains(&home));

    // With expand-path denied, ~ resolves using the SFTP server's initial cwd,
    // not the client's HOME (which is deliberately different in this fixture).
    let response = daemon.command("open sftp://user@noexpand.invalid/~");
    daemon.finish(response, true);
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {}/", daemon.remote().display())));
    let response = daemon.command("open sftp://user@noexpand.invalid/~/photos");
    daemon.finish(response, true);
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {}/photos/", daemon.remote().display())));
}

#[test]
fn password_is_reused_for_transport_recovery_even_without_an_attached_client() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    daemon.connect("test-secret", true);
    let response = daemon.command("cd photos");
    daemon.finish(response, true);
    let cwd = format!("CWD: {}/photos/", daemon.remote().display());
    let downloads = daemon.root.path().join("downloads");
    fs::create_dir(&downloads).unwrap();
    writeln!(
        daemon.stream,
        "LOCAL_CWD {}",
        serde_json::to_string(&downloads).unwrap()
    )
    .unwrap();
    assert_eq!(daemon.line(), "LOCAL_CWD_OK");
    let pids = fs::read_to_string(daemon.root.path().join("ssh-pids")).unwrap();
    let pid = pids.lines().last().unwrap().parse::<i32>().unwrap();
    // SAFETY: kill the private fake-SSH/SFTP process recorded by this fixture.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let response = daemon.command("get image.txt");
    // Reauthentication must work in the daemon, without prompting a client.
    daemon.stream.write_all(b"DETACH\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fs::read(downloads.join("image.txt")).is_ok_and(|data| data == b"image") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "download did not recover while detached"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    daemon.reattach();
    daemon.finish(response, true);
    assert!(daemon.command("status").contains(&cwd));
    assert_eq!(
        fs::read_to_string(daemon.root.path().join("ssh-pids"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    let paths = fs::read_to_string(daemon.root.path().join("askpass-paths")).unwrap();
    assert_eq!(paths.lines().count(), 2);
    for path in paths.lines() {
        assert!(
            !Path::new(path).parent().unwrap().exists(),
            "askpass helper leaked"
        );
    }
    let response = daemon.command("close");
    daemon.finish(response, true);
}
