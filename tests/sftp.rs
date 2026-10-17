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
    socket: PathBuf,
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
    *readonly.invalid*) exec {server} -R -d "$TEST_REMOTE_ROOT";;
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_xfer"))
            .arg("daemon")
            .env(xfer::daemon::ALLOW_FOREIGN_PARENT_ENV, "1")
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
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        let ready: serde_json::Value = serde_json::from_str(
            &receiver
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ready["pid"], child.id());
        let socket = PathBuf::from(ready["socket"].as_str().unwrap());
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
            socket,
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
        let socket = &self.socket;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut stream = UnixStream::connect(socket).unwrap();
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
    let response = daemon.command("ls -lF");
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
    // The fixture's server runs locally, so host ls is a valid reference for
    // server-provided owner/group names. Production never resolves IDs locally.
    let reference = Command::new("ls")
        .args(["-ld", "--"])
        .arg(daemon.remote().join("hello world.txt"))
        .env("LC_ALL", "C")
        .output()
        .unwrap();
    assert!(reference.status.success());
    let reference = String::from_utf8(reference.stdout).unwrap();
    let reference = reference.split_whitespace().collect::<Vec<_>>();
    assert!(fields[1] == reference[2] || fields[1] == metadata.uid().to_string());
    assert!(fields[2] == reference[3] || fields[2] == metadata.gid().to_string());
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

#[test]
fn lcd_uses_daemon_home_and_changes_future_download_destinations_only() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let home = daemon.root.path().join("home");
    let downloads = home.join("photo album");
    fs::create_dir_all(&downloads).unwrap();
    daemon.connect("test-secret", true);
    let response = daemon.command("cd photos");
    daemon.finish(response, true);
    assert_eq!(
        daemon.command("lcd"),
        vec![format!("Local CWD: {}", home.display())]
    );
    assert_eq!(
        daemon.command("lcd '~//photo album'"),
        vec![format!("Local CWD: {}", downloads.display())]
    );
    let response = daemon.command("get image.txt");
    // A new lcd must not redirect a download already submitted by the client.
    assert_eq!(
        daemon.command("lcd .."),
        vec![format!("Local CWD: {}", home.display())]
    );
    daemon.finish(response, true);
    assert_eq!(fs::read(downloads.join("image.txt")).unwrap(), b"image");
    assert!(!home.join("image.txt").exists());
    let status = daemon.command("status");
    assert!(status.contains(&format!("Local CWD: {}", home.display())));
    assert!(status.contains(&format!("CWD: {}/photos/", daemon.remote().display())));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn ls_flags_control_names_classification_metadata_visibility_and_sorting() {
    fn ls(daemon: &mut Daemon, command: &str) -> Vec<String> {
        let response = daemon.command(command);
        let output = daemon.finish(response, true);
        assert!(output[0].starts_with("OFFSET "));
        output.into_iter().skip(1).collect()
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    fs::write(daemon.remote().join(".secret"), b"secret").unwrap();
    fs::create_dir(daemon.remote().join(".private")).unwrap();
    fs::create_dir(daemon.remote().join("adir")).unwrap();
    daemon.connect("test-secret", true);
    let names = vec![
        "adir",
        "linked photos",
        "photos",
        "hello world.txt",
        "large.bin",
    ];
    assert!(
        daemon.command("show ui:ls-default-options")[0].starts_with("ui:ls-default-options: -lF")
    );
    let defaults = ls(&mut daemon, "ls");
    assert!(defaults[0].starts_with('d') && defaults[0].ends_with("  adir/"));
    assert_eq!(
        ls(&mut daemon, "ls -r"),
        defaults.iter().rev().cloned().collect::<Vec<_>>()
    );
    assert_eq!(ls(&mut daemon, "ls --no-long --no-classify"), names);
    assert!(ls(&mut daemon, "ls --no-classify")[0].ends_with("  adir"));
    daemon.command("set ui:ls-default-options ''");
    assert_eq!(ls(&mut daemon, "ls"), names);
    assert_eq!(
        ls(&mut daemon, "ls -F"),
        vec![
            "adir/",
            "linked photos/",
            "photos/",
            "hello world.txt",
            "large.bin"
        ]
    );
    assert_eq!(
        ls(&mut daemon, "ls -r"),
        names.iter().rev().copied().collect::<Vec<_>>()
    );
    assert_eq!(
        ls(&mut daemon, "ls -a"),
        vec![
            ".private",
            "adir",
            "linked photos",
            "photos",
            ".secret",
            "hello world.txt",
            "large.bin"
        ]
    );
    let mut by_size = names.clone();
    by_size.sort_by(|a, b| {
        fs::symlink_metadata(daemon.remote().join(b))
            .unwrap()
            .len()
            .cmp(&fs::symlink_metadata(daemon.remote().join(a)).unwrap().len())
            .then_with(|| a.cmp(b))
    });
    assert_eq!(ls(&mut daemon, "ls -S"), by_size);
    assert_eq!(
        ls(&mut daemon, "ls -Sr"),
        by_size.into_iter().rev().collect::<Vec<_>>()
    );
    let long = ls(&mut daemon, "ls -l");
    assert!(long[0].starts_with('d') && long[0].ends_with("  adir"));
    assert!(long[1].starts_with('l') && long[1].ends_with("  linked photos"));
    assert!(long[3].starts_with("-rw-r-----") && long[3].ends_with("18  hello world.txt"));
    let all = ls(&mut daemon, "ls -laF");
    assert!(all[0].ends_with("  .private/"));
    assert!(all.iter().any(|line| line.ends_with("  .secret")));
    assert_eq!(ls(&mut daemon, "ls .secret"), vec![".secret"]);
    assert_eq!(ls(&mut daemon, "ls adir/"), Vec::<String>::new());
    // Hidden records remain in the shared typed cache despite display filtering.
    ls(&mut daemon, "ls");
    daemon.stream.write_all(b"COMPLETE_LS \".s\"\n").unwrap();
    let line = daemon.line();
    let completion: xfer::cache::PathCompletion =
        serde_json::from_str(line.strip_prefix("COMPLETION ").unwrap()).unwrap();
    assert_eq!(completion.candidates, vec![".secret"]);
    daemon.command("set ui:ls-default-options '-laFSr'");
    assert_eq!(
        ls(
            &mut daemon,
            "ls --no-long --no-classify --no-all --no-size --no-reverse"
        ),
        names
    );
    daemon.command("set ui:ls-default-options ''");
    fs::write(daemon.remote().join("-literal"), b"dash").unwrap();
    assert_eq!(ls(&mut daemon, "ls -F -- -literal"), vec!["-literal"]);
    assert!(daemon.command("ls -z")[0].starts_with("unknown ls option -z"));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn cat_reads_remote_files_as_exact_bytes_and_recovers_a_dead_transport() {
    fn cat(daemon: &mut Daemon, command: &str, success: bool) -> Vec<u8> {
        let response = daemon.command(command);
        let id = response[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let status = daemon.command(&format!("status {id}"));
            assert!(status[0].split_whitespace().nth(2) == Some("RAW"));
            if status[0].contains("Completed") || status[0].contains("Failed") {
                assert_eq!(status[0].contains("Completed"), success, "{status:?}");
                break;
            }
            assert!(Instant::now() < deadline, "cat did not finish: {status:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut data = Vec::new();
        loop {
            let response = daemon.command(&format!("read-output {id} {}", data.len()));
            let (next, bytes) = response[0]
                .strip_prefix("CHUNK ")
                .unwrap()
                .split_once(' ')
                .unwrap();
            let bytes: Vec<u8> = serde_json::from_str(bytes).unwrap();
            assert!(bytes.len() <= 64 * 1024);
            assert_eq!(next.parse::<usize>().unwrap(), data.len() + bytes.len());
            if bytes.is_empty() {
                break;
            }
            data.extend(bytes);
        }
        assert_eq!(
            daemon.command(&format!("ack-output {id}")),
            vec!["output acknowledged"]
        );
        data
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    daemon.connect("test-secret", true);
    let data = (0..200_000).map(|n| (n % 256) as u8).collect::<Vec<_>>();
    fs::write(daemon.remote().join("binary.bin"), &data).unwrap();
    fs::write(daemon.remote().join("empty"), b"").unwrap();
    fs::write(daemon.remote().join("photos/-literal"), b"dash").unwrap();
    assert_eq!(
        cat(
            &mut daemon,
            "cat 'hello world.txt' empty photos/image.txt",
            true
        ),
        b"hello remote worldimage"
    );
    assert_eq!(cat(&mut daemon, "cat empty", true), b"");
    let response = daemon.command("cd photos");
    daemon.finish(response, true);
    assert_eq!(
        cat(&mut daemon, "cat image.txt '../hello world.txt'", true),
        b"imagehello remote world"
    );
    let pids = fs::read_to_string(daemon.root.path().join("ssh-pids")).unwrap();
    let pid = pids.lines().last().unwrap().parse::<i32>().unwrap();
    // SAFETY: terminate only the private SSH/SFTP transport created by this fixture.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    assert_eq!(cat(&mut daemon, "cat ../binary.bin", true), data);
    assert_eq!(cat(&mut daemon, "cat -- -literal", true), b"dash");
    assert_eq!(cat(&mut daemon, "cat image.txt missing", false), b"image");
    assert_eq!(cat(&mut daemon, "cat .", false), b"");
    assert!(daemon.command("cat")[0].starts_with("usage: cat"));
    assert!(daemon.command("cat -n file")[0].starts_with("unknown cat option"));
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {}/photos/", daemon.remote().display())));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn mkdir_creates_remote_directories_parents_and_refreshes_completion_after_reconnect() {
    fn complete(daemon: &mut Daemon, path: &str) -> Vec<String> {
        writeln!(
            daemon.stream,
            "COMPLETE_CD {}",
            serde_json::to_string(path).unwrap()
        )
        .unwrap();
        let response = daemon.line();
        let result: xfer::cache::PathCompletion =
            serde_json::from_str(response.strip_prefix("COMPLETION ").unwrap()).unwrap();
        result.candidates
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    daemon.connect("test-secret", true);
    let before = daemon.command("status");
    let local = before
        .iter()
        .find(|line| line.starts_with("Local CWD:"))
        .unwrap()
        .clone();
    let cwd = before
        .iter()
        .find(|line| line.starts_with("CWD:"))
        .unwrap()
        .clone();
    assert!(complete(&mut daemon, "sing").is_empty());
    let response = daemon.command("mkdir single second");
    assert_eq!(daemon.finish(response, true), vec!["OFFSET 0"]);
    assert!(daemon.remote().join("single").is_dir());
    assert!(daemon.remote().join("second").is_dir());
    assert_eq!(complete(&mut daemon, "sing"), vec!["single/"]);
    for command in [
        "mkdir single",
        "mkdir absent/child",
        "mkdir -p 'hello world.txt'",
        "mkdir -p 'hello world.txt/child'",
    ] {
        let response = daemon.command(command);
        assert!(!daemon.finish(response, false).is_empty());
    }
    assert!(!daemon.remote().join("absent").exists());
    assert_eq!(
        fs::read(daemon.remote().join("hello world.txt")).unwrap(),
        b"hello remote world"
    );
    let response = daemon.command("mkdir -p tree/multiple/subdirs/at/once 'dir with spaces/child'");
    daemon.finish(response, true);
    assert!(daemon
        .remote()
        .join("tree/multiple/subdirs/at/once")
        .is_dir());
    assert!(daemon.remote().join("dir with spaces/child").is_dir());
    let response = daemon.command("mkdir -p tree/multiple/subdirs/at/once . /");
    daemon.finish(response, true);
    assert!(complete(&mut daemon, "photos/thr").is_empty());
    let response = daemon.command("mkdir -p 'linked photos/through-link/nested'");
    daemon.finish(response, true);
    assert!(daemon.remote().join("photos/through-link/nested").is_dir());
    assert_eq!(
        complete(&mut daemon, "photos/thr"),
        vec!["photos/through-link/"]
    );
    std::os::unix::fs::symlink("missing-target", daemon.remote().join("broken")).unwrap();
    let response = daemon.command("mkdir -p broken/child");
    daemon.finish(response, false);
    assert!(!daemon.remote().join("missing-target").exists());
    let absolute = daemon.remote().join("absolute/child");
    let response = daemon.command(&format!(
        "mkdir -p {}",
        shlex::try_quote(absolute.to_str().unwrap()).unwrap()
    ));
    daemon.finish(response, true);
    assert!(absolute.is_dir());
    let status = daemon.command("status");
    assert!(status.contains(&local));
    assert!(status.contains(&cwd));
    let response = daemon.command("cd photos");
    daemon.finish(response, true);
    let response = daemon.command("mkdir -- -literal");
    daemon.finish(response, true);
    assert!(daemon.remote().join("photos/-literal").is_dir());
    let response = daemon.command("mkdir -p ../siblings/child");
    daemon.finish(response, true);
    assert!(daemon.remote().join("siblings/child").is_dir());
    let pids = fs::read_to_string(daemon.root.path().join("ssh-pids")).unwrap();
    let pid = pids.lines().last().unwrap().parse::<i32>().unwrap();
    // SAFETY: terminate only the private transport created by this fixture.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let response = daemon.command("mkdir -p recovered/deep");
    daemon.finish(response, true);
    assert!(daemon.remote().join("photos/recovered/deep").is_dir());
    assert!(!daemon.remote().join("recovered").exists());
    let status = daemon.command("status");
    assert!(status.contains(&format!("CWD: {}/photos/", daemon.remote().display())));
    assert!(status.contains(&local));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn mkdir_rejects_permission_failures_but_parents_accepts_existing_readonly_directories() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let response = daemon.command("open sftp://readonly.invalid");
    daemon.finish(response, true);
    for command in ["mkdir blocked", "mkdir -p blocked/child"] {
        let response = daemon.command(command);
        let output = daemon.finish(response, false).join("\n");
        assert!(output.contains("permission denied"), "{output}");
    }
    assert!(!daemon.remote().join("blocked").exists());
    let response = daemon.command("mkdir -p photos");
    daemon.finish(response, true);
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn local_cwd_query_tracks_lcd_and_symlink_resolution_without_changing_remote_cwd() {
    fn local_cwd(daemon: &mut Daemon) -> PathBuf {
        writeln!(daemon.stream, "LOCAL_CWD").unwrap();
        let response = daemon.line();
        serde_json::from_str(response.strip_prefix("LOCAL_CWD ").unwrap()).unwrap()
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    daemon.connect("test-secret", true);
    let remote = format!("CWD: {}/", daemon.remote().display());
    let selected = daemon.root.path().join("Pobrane 'local' 日本語");
    fs::create_dir(&selected).unwrap();
    let alias = daemon.root.path().join("Downloads");
    std::os::unix::fs::symlink(&selected, &alias).unwrap();
    let selected = selected.canonicalize().unwrap();
    assert_eq!(
        daemon.command(&format!(
            "lcd {}",
            shlex::try_quote(alias.to_str().unwrap()).unwrap()
        )),
        vec![format!("Local CWD: {}", selected.display())]
    );
    assert_eq!(local_cwd(&mut daemon), selected);
    assert!(daemon.command("lcd missing")[0].starts_with("cannot change local directory"));
    assert_eq!(local_cwd(&mut daemon), selected);
    assert!(daemon.command("status").contains(&remote));
    daemon.command("lcd ..");
    assert_eq!(
        local_cwd(&mut daemon),
        daemon.root.path().canonicalize().unwrap()
    );
    assert!(daemon.command("status").contains(&remote));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn lcd_completion_reads_fresh_local_directories_even_without_a_remote_connection() {
    fn complete(daemon: &mut Daemon, path: &str) -> xfer::cache::PathCompletion {
        writeln!(
            daemon.stream,
            "COMPLETE_LCD {}",
            serde_json::to_string(path).unwrap()
        )
        .unwrap();
        let response = daemon.line();
        serde_json::from_str(response.strip_prefix("COMPLETION ").unwrap()).unwrap()
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let local = daemon.root.path().join("local");
    fs::create_dir_all(local.join("photo album 日本語")).unwrap();
    fs::create_dir(local.join(".private")).unwrap();
    fs::write(local.join("photo.txt"), b"not a directory").unwrap();
    std::os::unix::fs::symlink("photo album 日本語", local.join("linked photos")).unwrap();
    fs::create_dir_all(daemon.root.path().join("home/Downloads/nested")).unwrap();
    daemon.command(&format!(
        "lcd {}",
        shlex::try_quote(local.to_str().unwrap()).unwrap()
    ));
    assert_eq!(
        complete(&mut daemon, "pho").candidates,
        vec!["photo album 日本語/"]
    );
    assert_eq!(
        complete(&mut daemon, "").candidates,
        vec!["linked photos/", "photo album 日本語/"]
    );
    assert_eq!(
        complete(&mut daemon, "~/Do").candidates,
        vec!["~/Downloads/"]
    );
    assert_eq!(complete(&mut daemon, "~").candidates, vec!["~/"]);
    assert_eq!(complete(&mut daemon, ".pri").candidates, vec![".private/"]);
    assert!(complete(&mut daemon, "new").candidates.is_empty());
    fs::create_dir(local.join("new directory")).unwrap();
    assert_eq!(
        complete(&mut daemon, "new").candidates,
        vec!["new directory/"]
    );
    assert!(daemon.command("jobs").is_empty());
    daemon.connect("test-secret", true);
    // The server has photos/, but lcd must still use the local filesystem.
    assert_eq!(
        complete(&mut daemon, "pho").candidates,
        vec!["photo album 日本語/"]
    );
    writeln!(daemon.stream, "COMPLETE_LCD \"missing/\"").unwrap();
    assert!(daemon.line().starts_with("COMPLETION_ERROR "));
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {}/", daemon.remote().display())));
    daemon.command("lcd ~/Downloads");
    assert_eq!(complete(&mut daemon, "ne").candidates, vec!["nested/"]);
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn put_completion_is_local_file_only_and_selected_files_upload_from_local_cwd() {
    fn complete(daemon: &mut Daemon, input: &str) -> Vec<String> {
        writeln!(
            daemon.stream,
            "COMPLETE_PUT {}",
            serde_json::to_string(input).unwrap()
        )
        .unwrap();
        let response = daemon.line();
        let completion: xfer::cache::PathCompletion =
            serde_json::from_str(response.strip_prefix("COMPLETION ").unwrap()).unwrap();
        completion.candidates
    }
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    let local = daemon.root.path().join("local files");
    fs::create_dir_all(local.join("photo directory")).unwrap();
    let data = b"local\0\xfffile bytes";
    fs::write(local.join("photo album 日本語.txt"), data).unwrap();
    fs::write(local.join("-dash"), b"literal dash").unwrap();
    std::os::unix::fs::symlink("photo album 日本語.txt", local.join("photo link")).unwrap();
    fs::create_dir_all(daemon.root.path().join("home/Downloads")).unwrap();
    fs::write(
        daemon.root.path().join("home/Downloads/home.txt"),
        b"home file",
    )
    .unwrap();
    daemon.command(&format!(
        "lcd {}",
        shlex::try_quote(local.to_str().unwrap()).unwrap()
    ));
    let file = complete(&mut daemon, "pho");
    assert_eq!(file, vec!["photo album 日本語.txt"]);
    assert_eq!(
        complete(&mut daemon, ""),
        vec!["./-dash", "photo album 日本語.txt"]
    );
    assert!(daemon.command("jobs").is_empty());
    let home = complete(&mut daemon, "~/Downloads/ho");
    assert_eq!(home, vec!["~/Downloads/home.txt"]);
    daemon.connect("test-secret", true);
    // Prime remote completion to verify upload invalidates its old snapshot.
    writeln!(daemon.stream, "COMPLETE_GET \"photo album\"").unwrap();
    let response = daemon.line();
    let old: xfer::cache::PathCompletion =
        serde_json::from_str(response.strip_prefix("COMPLETION ").unwrap()).unwrap();
    assert!(old.candidates.is_empty());
    let response = daemon.command(&format!(
        "put {} {}",
        shlex::try_quote(&file[0]).unwrap(),
        shlex::try_quote(&home[0]).unwrap()
    ));
    daemon.finish(response, true);
    assert_eq!(fs::read(daemon.remote().join(&file[0])).unwrap(), data);
    assert_eq!(
        fs::read(daemon.remote().join("home.txt")).unwrap(),
        b"home file"
    );
    writeln!(daemon.stream, "COMPLETE_GET \"photo album\"").unwrap();
    let response = daemon.line();
    let updated: xfer::cache::PathCompletion =
        serde_json::from_str(response.strip_prefix("COMPLETION ").unwrap()).unwrap();
    assert_eq!(updated.candidates, file);
    assert_eq!(complete(&mut daemon, "-da"), vec!["-dash"]);
    let response = daemon.command("put -- -dash");
    daemon.finish(response, true);
    assert_eq!(
        fs::read(daemon.remote().join("-dash")).unwrap(),
        b"literal dash"
    );
    let other = daemon.root.path().join("other local");
    fs::create_dir(&other).unwrap();
    fs::write(other.join("alternate.txt"), b"data").unwrap();
    daemon.command(&format!(
        "lcd {}",
        shlex::try_quote(other.to_str().unwrap()).unwrap()
    ));
    assert!(complete(&mut daemon, "pho").is_empty());
    assert_eq!(complete(&mut daemon, "alt"), vec!["alternate.txt"]);
    assert!(daemon
        .command("status")
        .contains(&format!("CWD: {}/", daemon.remote().display())));
    let response = daemon.command("close");
    daemon.finish(response, true);
}

#[test]
fn stored_sftp_bookmark_password_uses_askpass_without_prompt_or_credential_output() {
    let Some(mut daemon) = Daemon::start() else {
        return;
    };
    fs::write(
        daemon.root.path().join("config/xfer/bookmarks.toml"),
        "[stored]\ntype='sftp'\nurl='sftp://user@example.invalid'\npassword='test-secret'\n",
    )
    .unwrap();
    let response = daemon.command("open stored");
    assert!(response[0].starts_with("JOB "));
    daemon.finish(response, true);
    for command in ["bookmark", "jobs", "status"] {
        assert!(!daemon.command(command).join("\n").contains("test-secret"));
    }
    let response = daemon.command("close");
    daemon.finish(response, true);
}
