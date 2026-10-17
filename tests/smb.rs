//! Optional real SMB2/3 integration. Samba listens only on an ephemeral loopback
//! port and uses private state/passdb files, never the system server or accounts.
//! Non-root Linux runs use an unprivileged user namespace when available.
#![cfg(target_os = "linux")]
use std::{
    fs,
    io::Write,
    net::{TcpListener, TcpStream},
    os::unix::process::CommandExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use xfer::{
    backend::{Backend, ConnectOptions, Smb},
    core::{Daemon, JobState, Settings},
    credentials::Password,
    error::NonRetryableError,
    listing::{Content, ListingSink},
    transfer::DownloadSink,
};

const SECRET: &str = "smb-fixture-secret";
struct Server {
    root: tempfile::TempDir,
    child: Child,
    port: u16,
}
impl Drop for Server {
    fn drop(&mut self) {
        // Samba's RPC helper daemonizes into a different process group. Match
        // only its exact private config argument, never the system Samba server.
        let config = format!(
            "--configfile={}",
            self.root.path().join("smb.conf").display()
        );
        if let Ok(processes) = fs::read_dir("/proc") {
            for process in processes.flatten() {
                let Ok(name) = fs::read_to_string(process.path().join("comm")) else {
                    continue;
                };
                if name.trim() != "samba-dcerpcd" && !name.starts_with("rpcd_") {
                    continue;
                }
                let Ok(command) = fs::read(process.path().join("cmdline")) else {
                    continue;
                };
                if !command
                    .split(|byte| *byte == 0)
                    .any(|arg| arg == config.as_bytes())
                {
                    continue;
                }
                if let Ok(pid) = process.file_name().to_string_lossy().parse::<i32>() {
                    // SAFETY: only our fixture's isolated RPC processes match.
                    unsafe {
                        libc::kill(pid, libc::SIGTERM);
                    }
                }
            }
        }
        // SAFETY: this is our child's private process group, established on spawn.
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // Reap workers too, including if the parent exited first.
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        let _ = self.child.wait();
    }
}
fn sandbox(program: &str) -> Command {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        Command::new(program)
    } else {
        let mut command = Command::new("unshare");
        command.args([
            "--map-auto",
            "--map-root-user",
            "setpriv",
            "--clear-groups",
            program,
        ]);
        command
    }
}
impl Server {
    fn start() -> Option<Self> {
        if !Command::new("smbd")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
            || !sandbox("true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success())
        {
            eprintln!("skipping SMB integration: needs Samba and root or working unshare --map-auto / setpriv");
            return None;
        }
        let root = tempfile::tempdir().unwrap();
        for name in [
            "share", "second", "private", "lock", "state", "cache", "run", "ncalrpc",
        ] {
            fs::create_dir(root.path().join(name)).unwrap();
        }
        fs::create_dir_all(root.path().join("share/photos")).unwrap();
        fs::write(
            root.path().join("share/hello world.txt"),
            b"hello SMB world",
        )
        .unwrap();
        fs::write(root.path().join("share/photos/image.txt"), b"image").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let path = root.path().display();
        let config = root.path().join("smb.conf");
        fs::write(&config, format!("[global]\nserver role = standalone server\nworkgroup = WORKGROUP\nnetbios name = XFERTEST\ninterfaces = 127.0.0.1\nbind interfaces only = yes\nsmb ports = {port}\nserver min protocol = SMB2_02\nserver smb encrypt = desired\nsecurity = user\nmap to guest = Never\nguest account = root\nprivate dir = {path}/private\nlock directory = {path}/lock\nstate directory = {path}/state\ncache directory = {path}/cache\npid directory = {path}/run\nncalrpc dir = {path}/ncalrpc\nlog file = {path}/log.%m\nload printers = no\nprinting = bsd\nprintcap name = /dev/null\n[share]\npath = {path}/share\nguest ok = yes\nread only = no\n[second]\npath = {path}/second\nguest ok = yes\nread only = no\n[readonly]\npath = {path}/share\nguest ok = yes\nread only = yes\n[encrypted]\npath = {path}/share\nguest ok = no\nread only = no\nsmb encrypt = required\n")).unwrap();
        let mut password = sandbox("smbpasswd")
            .args(["-s", "-a", "-c"])
            .arg(&config)
            .arg("root")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        {
            let mut stdin = password.stdin.take().unwrap();
            writeln!(stdin, "{SECRET}\n{SECRET}").unwrap();
        }
        if !password.wait().unwrap().success() {
            eprintln!("skipping SMB integration: private passdb setup unavailable");
            return None;
        }
        let log = fs::File::create(root.path().join("stderr")).unwrap();
        let mut command = sandbox("smbd");
        command
            .args(["-F", "--no-process-group", "-s"])
            .arg(&config)
            .arg("-l")
            .arg(root.path())
            .stdin(Stdio::piped())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0);
        // SAFETY: only async-signal-safe setrlimit runs in the forked child.
        unsafe {
            command.pre_exec(|| {
                let limit = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::setrlimit(libc::RLIMIT_CORE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().unwrap();
        let mut server = Self { root, child, port };
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            if server.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                panic!(
                    "isolated Samba did not start: {}",
                    fs::read_to_string(server.root.path().join("log.smbd")).unwrap_or_default()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Some(server)
    }
    fn url(&self, user: bool, path: &str) -> String {
        format!(
            "smb://{}127.0.0.1:{}/{}",
            if user { "WORKGROUP;root@" } else { "" },
            self.port,
            path
        )
    }
    fn options(&self, user: bool, path: &str) -> ConnectOptions {
        let mut settings = Settings::new();
        Smb.register_settings(&mut settings);
        ConnectOptions {
            target: self.url(user, path),
            bookmark: None,
            settings,
            password: user.then(|| Password::new(SECRET.into())),
        }
    }
    fn path(&self, path: &str) -> PathBuf {
        self.root.path().join(path)
    }
}
#[derive(Default)]
struct Records {
    entries: Vec<Content>,
    explicit: bool,
}
impl ListingSink for Records {
    fn emit(&mut self, entry: Content) -> anyhow::Result<()> {
        self.entries.push(entry);
        Ok(())
    }
    fn emit_file(&mut self, entry: Content) -> anyhow::Result<()> {
        self.explicit = true;
        self.emit(entry)
    }
}
#[derive(Default)]
struct Bytes(Vec<u8>);
impl DownloadSink for Bytes {
    fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}
async fn finish(app: &mut Daemon, result: Vec<String>, success: bool) -> u64 {
    let id = result[0]
        .strip_prefix("JOB ")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while app.has_running_jobs() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("SMB job did not finish");
    assert_eq!(
        app.jobs.jobs[&id].state == JobState::Completed,
        success,
        "{:?}",
        app.jobs.jobs[&id].output.read_all()
    );
    assert!(!app.jobs.jobs[&id].title.contains(SECRET));
    assert!(!app.jobs.jobs[&id]
        .output
        .read_all()
        .join("\n")
        .contains(SECRET));
    id
}

#[tokio::test]
async fn smb_guest_navigation_listing_download_upload_and_mkdir() {
    let Some(server) = Server::start() else {
        return;
    };
    let mut connection = Smb.connect(server.options(false, "share")).await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/share/");
    let mut records = Records::default();
    connection.list("", &mut records).await.unwrap();
    assert!(records.entries.iter().any(|entry| matches!(entry, Content::Directory(info) if info.name == "photos" && info.unix.is_none())));
    assert!(records.entries.iter().any(|entry| matches!(entry, Content::File(info) if info.name == "hello world.txt" && info.size == Some(15) && info.unix.is_none() && info.modified.is_some())));
    let mut file = Records::default();
    connection.list("hello world.txt", &mut file).await.unwrap();
    assert!(file.explicit);
    let metadata = connection.stat_file("hello world.txt").await.unwrap();
    let mut bytes = Bytes::default();
    connection
        .download("hello world.txt", 6, &metadata, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes.0, b"SMB world");
    connection.change_directory("photos").await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/share/photos/");
    assert!(connection
        .change_directory("missing")
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert_eq!(connection.pwd().unwrap(), "/share/photos/");
    connection
        .create_directory("one/two/three", true)
        .await
        .unwrap();
    connection
        .create_directory("one/two/three", true)
        .await
        .unwrap();
    assert!(connection.create_directory("one", false).await.is_err());
    assert!(connection
        .create_directory("missing/child", false)
        .await
        .is_err());
    assert!(server.path("share/photos/one/two/three").is_dir());
    let local = server.path("local upload");
    fs::write(&local, b"upload\0\xffbytes").unwrap();
    connection
        .upload(&local, "one/two/three/upload.txt", false)
        .await
        .unwrap();
    assert_eq!(
        fs::read(server.path("share/photos/one/two/three/upload.txt")).unwrap(),
        b"upload\0\xffbytes"
    );
    assert!(connection
        .upload(&local, "one/two/three/upload.txt", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    fs::write(&local, b"replacement").unwrap();
    connection
        .upload(&local, "one/two/three/upload.txt", true)
        .await
        .unwrap();
    assert_eq!(
        fs::read(server.path("share/photos/one/two/three/upload.txt")).unwrap(),
        b"replacement"
    );
    connection.change_directory("/second").await.unwrap();
    connection.create_directory("new", false).await.unwrap();
    assert!(server.path("second/new").is_dir());
    assert!(connection
        .create_directory("/missing-share/new", true)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    connection.disconnect().await.unwrap();
}

#[tokio::test]
async fn smb_password_bookmarks_work_with_daemon_commands_and_failures_do_not_retry_or_leak() {
    let Some(server) = Server::start() else {
        return;
    };
    let mut app = Daemon::bootstrap().unwrap();
    let bookmarks = server.path("bookmarks.toml");
    fs::write(
        &bookmarks,
        format!(
            "[server]\ntype = 'smb'\nurl = '{}'\npassword_interactive = true\n",
            server.url(true, "share")
        ),
    )
    .unwrap();
    app.command(&format!(
        "set bookmarks:file {}",
        shlex::try_quote(bookmarks.to_str().unwrap()).unwrap()
    ));
    assert!(app.command("open server")[0].starts_with("PASSWORD_REQUIRED "));
    assert!(app.jobs.jobs.is_empty());
    let failed = app.connect_command("server", Some(Password::new("wrong secret".into())));
    finish(&mut app, failed, false).await;
    let connected = app.connect_command("server", Some(Password::new(SECRET.into())));
    finish(&mut app, connected, true).await;
    let local = server.path("downloads");
    fs::create_dir(&local).unwrap();
    app.set_local_directory(local.clone()).unwrap();
    fs::write(local.join("hello world.txt"), b"hello").unwrap();
    let response = app.command("get -c '/share/hello world.txt'");
    finish(&mut app, response, true).await;
    assert_eq!(
        fs::read(local.join("hello world.txt")).unwrap(),
        b"hello SMB world"
    );
    let response = app.command("mkdir -p deep/nested");
    finish(&mut app, response, true).await;
    let response = app.command("cd deep/nested");
    finish(&mut app, response, true).await;
    fs::write(local.join("upload 日本語.txt"), b"payload").unwrap();
    let response = app.command("put 'upload 日本語.txt'");
    finish(&mut app, response, true).await;
    let response = app.command("ls");
    let id = finish(&mut app, response, true).await;
    assert!(app.jobs.jobs[&id]
        .output
        .read_all()
        .join("\n")
        .contains("upload 日本語.txt"));
    let response = app.command("cat 'upload 日本語.txt'");
    let id = finish(&mut app, response, true).await;
    assert_eq!(
        fs::read(app.jobs.jobs[&id].output.path()).unwrap(),
        b"payload"
    );
    fs::remove_file(local.join("upload 日本語.txt")).unwrap();
    let response = app.command("get 'upload 日本語.txt'");
    finish(&mut app, response, true).await;
    assert_eq!(
        fs::read(local.join("upload 日本語.txt")).unwrap(),
        b"payload"
    );
    let response = app.command("du -s /share/deep/");
    let id = finish(&mut app, response, true).await;
    assert!(app.jobs.jobs[&id]
        .output
        .read_all()
        .join("\n")
        .contains("7"));
    let response = app.command("close");
    finish(&mut app, response, true).await;
}

#[tokio::test]
async fn smb_cancelled_reads_reconnect_and_resume_without_duplicate_bytes_and_reject_changed_files()
{
    let Some(server) = Server::start() else {
        return;
    };
    let data = (0..4 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(server.path("share/photos/large.bin"), &data).unwrap();
    let connection = Smb
        .connect(server.options(true, "share/photos"))
        .await
        .unwrap();
    let metadata = connection.stat_file("large.bin").await.unwrap();
    struct Signal {
        bytes: Bytes,
        notify: std::sync::Arc<tokio::sync::Notify>,
    }
    impl DownloadSink for Signal {
        fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
            self.bytes.write(bytes)?;
            self.notify.notify_one();
            Ok(())
        }
    }
    let notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let mut sink = Signal {
        bytes: Bytes::default(),
        notify: notify.clone(),
    };
    let mut download = Box::pin(connection.download("large.bin", 0, &metadata, &mut sink));
    tokio::select! { biased; _ = notify.notified() => {}, result = &mut download => panic!("read finished before cancellation: {result:?}"), }
    drop(download);
    assert!(!sink.bytes.0.is_empty() && sink.bytes.0.len() < data.len());
    let offset = sink.bytes.0.len() as u64;
    connection
        .download("large.bin", offset, &metadata, &mut sink.bytes)
        .await
        .unwrap();
    assert_eq!(sink.bytes.0, data);
    assert_eq!(connection.pwd().unwrap(), "/share/photos/");
    fs::write(server.path("share/photos/large.bin"), b"changed").unwrap();
    let mut empty = Bytes::default();
    assert!(connection
        .download("large.bin", 0, &metadata, &mut empty)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert!(empty.0.is_empty());
    connection.disconnect().await.unwrap();
}

#[tokio::test]
async fn smb_readonly_missing_shares_and_encryption_policy_are_permanent() {
    let Some(server) = Server::start() else {
        return;
    };
    let connection = Smb.connect(server.options(true, "readonly")).await.unwrap();
    assert!(connection
        .create_directory("no-write", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    connection.create_directory("photos", true).await.unwrap();
    connection.disconnect().await.unwrap();
    let result = Smb.connect(server.options(true, "does-not-exist")).await;
    assert!(result.err().unwrap().is::<NonRetryableError>());
    let mut options = server.options(false, "share");
    options
        .settings
        .values
        .get_mut("smb:require-encryption")
        .unwrap()
        .value = xfer::core::SettingValue::Bool(true);
    assert!(Smb
        .connect(options)
        .await
        .err()
        .unwrap()
        .is::<NonRetryableError>());
    let mut options = server.options(true, "encrypted");
    options
        .settings
        .values
        .get_mut("smb:require-encryption")
        .unwrap()
        .value = xfer::core::SettingValue::Bool(true);
    match Smb.connect(options).await {
        Ok(encrypted) => {
            let metadata = encrypted.stat_file("hello world.txt").await.unwrap();
            let mut bytes = Bytes::default();
            encrypted
                .download("hello world.txt", 0, &metadata, &mut bytes)
                .await
                .unwrap();
            assert_eq!(bytes.0, b"hello SMB world");
            encrypted.disconnect().await.unwrap();
        }
        Err(error) => {
            // smb2 0.31 / Samba 4.24 currently omit the encryption cipher
            // from negotiation. This MUST refuse the connection, not downgrade.
            assert!(error.is::<NonRetryableError>());
            assert!(
                error
                    .to_string()
                    .contains("did not negotiate SMB3 encryption"),
                "{error}"
            );
            eprintln!("encrypted SMB test verified fail-closed behavior: {error}");
        }
    }
}

#[tokio::test]
async fn smb_server_root_enumerates_shares_and_directories_preserve_initial_home() {
    let Some(server) = Server::start() else {
        return;
    };
    let mut connection = Smb.connect(server.options(true, "")).await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/");
    let mut listing = Records::default();
    connection.list("", &mut listing).await.unwrap();
    assert!(listing
        .entries
        .iter()
        .any(|entry| matches!(entry, Content::Directory(info) if info.name == "share")));
    assert!(!listing
        .entries
        .iter()
        .any(|entry| matches!(entry, Content::Directory(info) if info.name == "IPC$")));
    connection.change_directory("share/photos").await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/share/photos/");
    let home = connection.resolve_directory(None).unwrap();
    assert_eq!(home, "/");
    connection.change_directory(&home).await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/");
    connection.disconnect().await.unwrap();
}

#[tokio::test]
async fn smb_stored_credentials_and_explicit_anonymous_access_work_without_guest_mapping() {
    let Some(server) = Server::start() else {
        return;
    };
    let mut app = Daemon::bootstrap().unwrap();
    let books = server.path("stored-bookmarks.toml");
    let plain = server.url(false, "share");
    let password_url = server
        .url(true, "share")
        .replace('@', &format!(":{SECRET}@"));
    fs::write(&books, format!("[vm]\ntype='smb'\nurl='{password_url}'\nuser=''\ndomain=''\npassword=''\n[account]\ntype='smb'\nurl='{plain}'\nuser='root'\ndomain='WORKGROUP'\npassword='{SECRET}'\n[empty]\ntype='smb'\nurl='{password_url}'\npassword=''\n[ask]\ntype='smb'\nurl='{plain}'\nuser='root'\ndomain='WORKGROUP'\npassword='wrong-stored-password'\npassword_interactive=true\n")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&books, fs::Permissions::from_mode(0o600)).unwrap();
    app.command(&format!(
        "set bookmarks:file {}",
        shlex::try_quote(books.to_str().unwrap()).unwrap()
    ));
    app.command("set smb:user wrong-default");
    app.command("set smb:domain WRONG");
    assert!(!app
        .command("set smb:password 'wrong-global-password'")
        .join("\n")
        .contains("wrong-global-password"));
    // Empty bookmark user overrides the named URL and every configured default.
    let result = app.command("open vm");
    finish(&mut app, result, true).await;
    let result = app.command("ls");
    finish(&mut app, result, true).await;
    let result = app.command("open account");
    finish(&mut app, result, true).await;
    // Explicitly stored empty password must not fall back to the URL's password.
    let result = app.command("open empty");
    finish(&mut app, result, false).await;
    assert!(app.command("open ask")[0].starts_with("PASSWORD_REQUIRED "));
    let result = app.connect_command("ask", Some(Password::new(SECRET.into())));
    finish(&mut app, result, true).await;
    // URL shortcut beats global defaults, and never appears in job labels.
    let result = app.command(&format!("open {password_url}"));
    let id = finish(&mut app, result, true).await;
    assert!(!app.jobs.jobs[&id].title.contains("root@"));
    app.command("set smb:user root");
    app.command("set smb:domain WORKGROUP");
    app.command(&format!("set smb:password {SECRET}"));
    let result = app.command(&format!("open {plain}"));
    finish(&mut app, result, true).await;
    // @ deliberately supplies an empty user even with named global credentials.
    let anonymous_url = plain.replacen("smb://", "smb://@", 1);
    let result = app.command(&format!("open {anonymous_url}"));
    finish(&mut app, result, true).await;
    // The named Guest account is not a substitute for a NULL session.
    let guest_url = plain.replacen("smb://", "smb://guest:@", 1);
    let result = app.command(&format!("open {guest_url}"));
    finish(&mut app, result, false).await;
    for command in ["show", "bookmark", "jobs", "status"] {
        let display = app.command(command).join("\n");
        for secret in [SECRET, "wrong-global-password", "wrong-stored-password"] {
            assert!(!display.contains(secret), "{command} leaked a credential");
        }
    }
    let result = app.command("close");
    finish(&mut app, result, true).await;
}
