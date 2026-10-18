//! Actual daemon startup/publication and normal client-to-child attachment.
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use xfer::ipc::{self, pb, Op, Reply};

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
    fn attach(&self) -> ipc::Client<UnixStream> {
        let stream = UnixStream::connect(self.socket()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut client = ipc::Client::new(stream);
        client.attach().unwrap();
        client
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
        let mut client = server.attach();
        let session = client.session().unwrap();
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
    let mut client = existing.attach();
    assert!(matches!(
        client.call(Op::ClaimStartup(pb::Empty {})).unwrap(),
        Reply::Startup(pb::StartupResult { run: true })
    ));
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

fn raw_request(
    stream: &mut UnixStream,
    version: u32,
    id: u64,
    operation: Option<Op>,
) -> pb::Response {
    ipc::write_frame(
        stream,
        &pb::Request {
            version,
            id,
            operation,
        },
    )
    .unwrap();
    ipc::read_frame(stream).unwrap().unwrap()
}
fn error_code(response: pb::Response) -> pb::ErrorCode {
    let Some(Reply::Error(error)) = response.result else {
        panic!("expected RPC error")
    };
    pb::ErrorCode::try_from(error.code).unwrap()
}

#[test]
fn version_mismatch_is_explicit_and_does_not_claim_attachment() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut stream = UnixStream::connect(server.socket()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let response = raw_request(
        &mut stream,
        ipc::VERSION + 1,
        1,
        Some(Op::Attach(pb::Empty {})),
    );
    assert_eq!(response.version, ipc::VERSION);
    assert_eq!(response.id, 1);
    assert_eq!(error_code(response), pb::ErrorCode::UnsupportedVersion);
    assert!(ipc::read_frame::<pb::Response>(&mut stream)
        .unwrap()
        .is_none());
    let _client = server.attach();
}

#[test]
fn operations_require_attachment_and_ids_cannot_be_replayed() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut stream = UnixStream::connect(server.socket()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        error_code(raw_request(
            &mut stream,
            ipc::VERSION,
            1,
            Some(Op::Session(pb::Empty {}))
        )),
        pb::ErrorCode::NotAttached
    );
    assert_eq!(
        error_code(raw_request(&mut stream, ipc::VERSION, 2, None)),
        pb::ErrorCode::InvalidRequest
    );
    assert_eq!(
        error_code(raw_request(
            &mut stream,
            ipc::VERSION,
            2,
            Some(Op::Attach(pb::Empty {}))
        )),
        pb::ErrorCode::InvalidRequest
    );
    assert!(matches!(
        raw_request(&mut stream, ipc::VERSION, 3, Some(Op::Attach(pb::Empty {}))).result,
        Some(Reply::Attached(_))
    ));
    assert_eq!(
        error_code(raw_request(
            &mut stream,
            ipc::VERSION,
            4,
            Some(Op::Complete(pb::CompletionRequest {
                kind: 9999,
                input: String::new()
            }))
        )),
        pb::ErrorCode::InvalidRequest
    );
    assert!(matches!(
        raw_request(
            &mut stream,
            ipc::VERSION,
            5,
            Some(Op::Session(pb::Empty {}))
        )
        .result,
        Some(Reply::Session(_))
    ));
}

#[test]
fn malformed_oversized_truncated_and_legacy_frames_do_not_acquire_a_lease() {
    use std::io::{Read, Write};
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    for bytes in [
        0u32.to_be_bytes().to_vec(),
        (ipc::MAX_FRAME as u32 + 1).to_be_bytes().to_vec(),
        vec![0, 0, 0, 2, 0xff, 0xff],
        vec![0, 0, 0],
        vec![0, 0, 0, 10, 1, 2],
        b"ATTACH\n".to_vec(),
    ] {
        let mut stream = UnixStream::connect(server.socket()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(&bytes).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let result = stream.read(&mut [0]);
        assert!(
            matches!(result, Ok(0))
                || result.is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
        );
    }
    let _client = server.attach();
}

#[test]
fn single_attachment_and_probe_use_typed_results() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let stream = UnixStream::connect(server.socket()).unwrap();
    assert!(ipc::Client::new(stream).probe().unwrap());
    let _client = server.attach();
    let stream = UnixStream::connect(server.socket()).unwrap();
    let mut another = ipc::Client::new(stream);
    let error = another.attach().unwrap_err();
    assert_eq!(
        error.downcast_ref::<ipc::RemoteError>().unwrap().code,
        pb::ErrorCode::Busy
    );
    assert!(!another.probe().unwrap());
}

#[test]
fn job_progress_output_errors_acknowledgement_and_secret_settings_are_structured() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut client = server.attach();
    let secret = "not-in-any-response";
    let result = client
        .command(&format!("set smb:password {secret}"))
        .unwrap();
    assert!(!format!("{result:?}").contains(secret));
    let settings = client.settings(Some("smb:password")).unwrap();
    assert_eq!(
        settings[0].value,
        Some(pb::setting::Value::SecretIsSet(true))
    );
    assert!(!format!("{settings:?}").contains(secret));
    let result = client.command("pwd").unwrap();
    let Some(pb::command_result::Action::JobStarted(job)) = result.action else {
        panic!("expected job")
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = client.job(job.id).unwrap();
        assert!(!status.raw_output);
        assert!(status.progress.is_some());
        if status.state == pb::JobState::Failed as i32 {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = client.output(job.id, 0).unwrap();
    assert!(output.eof);
    assert!(String::from_utf8_lossy(&output.data).contains("not connected"));
    assert!(client.output(job.id, output.next_offset + 1).is_err());
    assert!(client.job(job.id).is_ok()); // semantic errors leave framing synchronized
    client
        .ok(Op::AcknowledgeOutput(pb::JobId { id: job.id }))
        .unwrap();
    assert_eq!(
        client
            .job(job.id)
            .unwrap_err()
            .downcast_ref::<ipc::RemoteError>()
            .unwrap()
            .code,
        pb::ErrorCode::NotFound
    );
    assert!(client.command("unknown command").is_err());
    assert!(client.session().is_ok());
}

#[test]
fn job_lists_are_paged_and_ordered_with_stable_cursors() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut client = server.attach();
    let mut expected = Vec::new();
    for _ in 0..5 {
        let result = client.command("pwd").unwrap();
        let Some(pb::command_result::Action::JobStarted(job)) = result.action else {
            panic!("expected job")
        };
        expected.push(job.id);
    }
    let mut after_id = 0;
    let mut actual = Vec::new();
    loop {
        let Reply::Jobs(page) = client
            .call(Op::Jobs(pb::JobsRequest { after_id, limit: 2 }))
            .unwrap()
        else {
            panic!("expected jobs")
        };
        assert!(page.jobs.len() <= 2);
        actual.extend(page.jobs.iter().map(|job| job.id));
        match page.next_after_id {
            Some(next) => {
                assert!(next > after_id);
                after_id = next;
            }
            None => break,
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn oversized_responses_return_a_bounded_error_without_desynchronizing_the_client() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut client = server.attach();
    let region = "a".repeat(ipc::MAX_FRAME - 512);
    client.command(&format!("set s3:region {region}")).unwrap();
    let error = client.settings(None).unwrap_err();
    assert_eq!(
        error.downcast_ref::<ipc::RemoteError>().unwrap().code,
        pb::ErrorCode::ResponseTooLarge
    );
    let single = client.settings(Some("s3:region")).unwrap();
    assert_eq!(single[0].value, Some(pb::setting::Value::Text(region)));
    assert!(client.session().is_ok());
}

#[test]
fn job_pages_also_obey_the_byte_budget_before_building_large_responses() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path());
    let mut client = server.attach();
    let command = format!("mkdir {}", "a".repeat(350_000));
    let mut expected = Vec::new();
    for _ in 0..3 {
        let result = client.command(&command).unwrap();
        let Some(pb::command_result::Action::JobStarted(job)) = result.action else {
            panic!("expected job")
        };
        expected.push(job.id);
    }
    let Reply::Jobs(first) = client
        .call(Op::Jobs(pb::JobsRequest {
            after_id: 0,
            limit: 256,
        }))
        .unwrap()
    else {
        panic!("expected jobs")
    };
    assert_eq!(first.jobs.len(), 2);
    let Reply::Jobs(second) = client
        .call(Op::Jobs(pb::JobsRequest {
            after_id: first.next_after_id.unwrap(),
            limit: 256,
        }))
        .unwrap()
    else {
        panic!("expected jobs")
    };
    assert!(second.next_after_id.is_none());
    assert_eq!(
        first
            .jobs
            .iter()
            .chain(&second.jobs)
            .map(|job| job.id)
            .collect::<Vec<_>>(),
        expected
    );
}
