//! Actual daemon startup/publication and normal client-to-child attachment.
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xfer"));
    command
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("XDG_RUNTIME_DIR", root.join("run"))
        .env_remove("XFER_CONFIG")
        .env_remove(xfer::ssh_askpass::SOCKET_ENV)
        .env_remove(xfer::daemon::ALLOW_FOREIGN_PARENT_ENV)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command
}

struct Server {
    child: Child,
    ready: serde_json::Value,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Server {
    fn start(root: &Path) -> Self {
        let child = command(root)
            .arg("daemon")
            .env(xfer::daemon::ALLOW_FOREIGN_PARENT_ENV, "1")
            .spawn()
            .unwrap();
        let mut server = Self {
            child,
            ready: serde_json::Value::Null,
        };
        let stdout = server.child.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        server.ready = serde_json::from_str(
            &receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(server.ready["pid"], server.child.id());
        server
    }

    fn socket(&self) -> PathBuf {
        self.ready["socket"].as_str().unwrap().into()
    }
    fn metadata(&self, root: &Path) -> PathBuf {
        root.join("run/xfer/instances")
            .join(format!("{}.meta", self.ready["id"].as_str().unwrap()))
    }
    fn attach(&self) -> (UnixStream, BufReader<UnixStream>) {
        let mut stream = UnixStream::connect(self.socket()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        stream.write_all(b"ATTACH\n").unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert_eq!(response.trim(), "ATTACHED");
        (stream, reader)
    }
}

fn wait(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not exit promptly");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn concurrent_daemons_announce_distinct_live_sockets_and_complete_metadata() {
    let root = tempfile::tempdir().unwrap();
    let first = Server::start(root.path());
    let second = Server::start(root.path());
    assert_ne!(first.ready["id"], second.ready["id"]);
    assert_ne!(first.socket(), second.socket());
    for server in [&first, &second] {
        let published: serde_json::Value =
            serde_json::from_slice(&fs::read(server.metadata(root.path())).unwrap()).unwrap();
        assert_eq!(published, server.ready);
        let (mut stream, mut reader) = server.attach();
        stream.write_all(b"SESSION\n").unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let session: xfer::daemon::SessionInfo =
            serde_json::from_str(line.strip_prefix("SESSION ").unwrap()).unwrap();
        assert_eq!(session.daemon_pid, server.child.id());
    }
}

#[test]
fn normal_client_attaches_to_its_own_child_not_an_existing_daemon() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("home")).unwrap();
    // Exit through the startup file, before the terminal editor needs a TTY.
    fs::write(root.path().join("home/.xferrc"), "detach\n").unwrap();
    let existing = Server::start(root.path());
    let mut client = command(root.path()).spawn().unwrap();
    assert!(wait(&mut client).success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let entries = fs::read_dir(root.path().join("run/xfer/instances"))
            .unwrap()
            .count();
        if entries == 2 {
            break;
        } // existing daemon's directory + metadata only
        assert!(
            Instant::now() < deadline,
            "new daemon did not clean up: {entries} entries"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(existing.metadata(root.path()).exists());
    let (mut stream, mut reader) = existing.attach();
    stream.write_all(b"STARTUP\n").unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    assert_eq!(response.trim(), "RUN_STARTUP");
}

#[test]
fn initialization_failure_never_announces_or_publishes_an_instance() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("config/xfer")).unwrap();
    fs::write(
        root.path().join("config/xfer/xfer.conf"),
        "invalid configuration line\n",
    )
    .unwrap();
    let mut child = command(root.path())
        .arg("daemon")
        .env(xfer::daemon::ALLOW_FOREIGN_PARENT_ENV, "1")
        .spawn()
        .unwrap();
    assert!(!wait(&mut child).success());
    let mut line = String::new();
    assert_eq!(
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap(),
        0
    );
    assert!(!root.path().join("run/xfer/instances").exists());
}
