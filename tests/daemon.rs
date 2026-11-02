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
        .current_dir(root)
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

#[test]
fn directory_snapshots_are_fresh_paged_and_closed_without_completion_limits() {
    let root = tempfile::tempdir().unwrap();
    let files = root.path().join("files");
    fs::create_dir(&files).unwrap();
    for index in 0..2500 {
        fs::write(files.join(format!("file{index}")), b"").unwrap();
    }
    fs::write(files.join(".hidden 日本語"), b"").unwrap();
    fs::create_dir(files.join("subdir")).unwrap();
    fs::write(files.join("subdir/not-recursive"), b"").unwrap();
    let server = Server::start(root.path());
    let mut client = server.attach();
    client
        .ok(Op::SetLocalCwd(pb::Path {
            path: files.to_str().unwrap().into(),
        }))
        .unwrap();
    let id = client
        .start_directory(pb::DirectorySource::Local, pb::DirectoryFilter::All)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.job(id).unwrap().state == pb::JobState::Running as i32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        client.job(id).unwrap().state,
        pb::JobState::Completed as i32
    );
    fs::write(files.join("created-after-snapshot"), b"").unwrap();
    // Canonicalize the expected base: daemon-resolved paths traverse macOS
    // symlinked temp roots (/var/folders -> /private/var/folders), while
    // `tempfile` hands out the unresolved spelling.
    let canonical_files = fs::canonicalize(&files).unwrap();
    let mut offset = 0;
    let mut names = Vec::new();
    loop {
        let page = client.directory_page(id, offset, 97).unwrap();
        assert_eq!(page.total, 2502);
        assert!(page.entries.len() <= 97);
        assert_eq!(page.path, canonical_files.to_str().unwrap());
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        offset = page.next_offset;
        if page.eof {
            break;
        }
    }
    assert_eq!(names.len(), 2502);
    assert!(names.windows(2).all(|pair| pair[0] <= pair[1]));
    let reversed = client
        .filter_directory_sorted(id, ".*", pb::DirectorySort::NameDesc)
        .unwrap();
    while client.job(reversed).unwrap().state == pb::JobState::Running as i32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let reverse_page = client.directory_page(reversed, 0, 97).unwrap();
    assert_eq!(reverse_page.entries[0].name, *names.last().unwrap());
    assert!(reverse_page
        .entries
        .windows(2)
        .all(|pair| pair[0].index > pair[1].index));
    assert!(reverse_page
        .entries
        .iter()
        .all(|entry| entry.directory || entry.size == Some(0)));
    client.close_directory(reversed).unwrap();

    assert!(names.contains(&".hidden 日本語".to_string()));
    assert!(!names
        .iter()
        .any(|name| name.contains("not-recursive") || name == "created-after-snapshot"));
    assert!(client.directory_page(id, offset + 1, 1).is_err());
    assert!(client.session().is_ok());
    let filtered = client.filter_directory(id, r"^file24[0-9]{2}$").unwrap();
    while client.job(filtered).unwrap().state == pb::JobState::Running as i32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let matches = client.directory_page(filtered, 0, 256).unwrap();
    assert_eq!(matches.total, 100);
    for entry in &matches.entries {
        assert_eq!(
            entry.index,
            names.iter().position(|name| name == &entry.name).unwrap() as u64
        );
    }
    assert!(client.filter_directory(id, "[").is_err());
    assert!(client
        .call(Op::FilterDirectory(pb::DirectoryRegexRequest {
            job_id: id,
            regexp: ".*".into(),
            sort: 999
        }))
        .is_err());

    assert!(client.filter_directory(id, &"x".repeat(4097)).is_err());
    assert!(client.session().is_ok());
    client.close_directory(filtered).unwrap();
    client.close_directory(id).unwrap();
    assert!(client.job(id).is_err());
    let id = client
        .start_directory(pb::DirectorySource::Local, pb::DirectoryFilter::Directories)
        .unwrap();
    while client.job(id).unwrap().state == pb::JobState::Running as i32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let page = client.directory_page(id, 0, 0).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "subdir");
    // Close also works immediately, without waiting for a new scan to finish.
    client.close_directory(id).unwrap();
    let id = client
        .start_directory(pb::DirectorySource::Local, pb::DirectoryFilter::All)
        .unwrap();
    client.close_directory(id).unwrap();
    assert!(client.job(id).is_err());
    let error = client
        .call(Op::StartDirectory(pb::DirectoryRequest {
            source: 0,
            filter: 0,
        }))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ipc::RemoteError>().unwrap().code,
        pb::ErrorCode::InvalidRequest
    );
    assert!(client.session().is_ok());
    // Snapshot-specific close/read operations cannot remove unrelated jobs.
    let result = client.command("pwd").unwrap();
    let Some(pb::command_result::Action::JobStarted(ordinary)) = result.action else {
        panic!()
    };
    assert!(client.close_directory(ordinary.id).is_err());
    assert!(client.directory_page(ordinary.id, 0, 1).is_err());
    assert!(client.job(ordinary.id).is_ok());
    assert!(client.session().is_ok());
}

#[test]
fn script_runs_without_a_terminal_and_stops_on_failed_job() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    fs::create_dir_all(path.join("home")).unwrap();
    let script = path.join("commands.rc");
    fs::write(
        &script,
        "# comment\nset s3:region eu-west-1\nshow s3:region\n!printf script-ok\\n\n",
    )
    .unwrap();
    let output = command(path)
        .arg("-f")
        .arg(&script)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("s3:region: eu-west-1"), "{stdout}");
    assert!(stdout.contains("script-ok"), "{stdout}");

    fs::write(&script, "pwd\n!touch must-not-run\n").unwrap();
    let output = command(path)
        .current_dir(path)
        .arg("-f")
        .arg(&script)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!path.join("must-not-run").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("commands.rc:1"));
}

#[test]
fn debug_controls_private_daemon_log_without_recording_arguments() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    fs::create_dir_all(path.join("home")).unwrap();
    let script = path.join("commands.rc");
    fs::write(&script, "debug\ndebug 2\nshow secret-argument\ndebug 0\nshow another-secret\n").unwrap();
    let output = command(path).arg("-f").arg(&script).stderr(Stdio::piped()).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("debug level: 0; log:"), "{stdout}");
    assert!(stdout.contains("debug level: 2; log:"), "{stdout}");
    let logs = fs::read_dir(path).unwrap().filter_map(Result::ok).filter(|entry| entry.file_name().to_string_lossy().starts_with("xfer-debug-")).collect::<Vec<_>>();
    assert_eq!(logs.len(), 1);
    let log_path = logs[0].path();
    assert_eq!(fs::metadata(&log_path).unwrap().permissions().mode() & 0o777, 0o600);
    let log = fs::read_to_string(log_path).unwrap();
    assert!(log.contains("TRACE dispatching command show"), "{log}");
    assert!(!log.contains("secret-argument"));
    assert!(!log.contains("another-secret"));
    assert_eq!(log.matches("TRACE dispatching command show").count(), 1);

    fs::write(&script, "debug 3\n").unwrap();
    let output = command(path).arg("-f").arg(&script).stderr(Stdio::piped()).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage: debug [0|1|2]"));
}

#[test]
fn debug_file_configuration_and_runtime_switch() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    fs::create_dir_all(path.join("home")).unwrap();
    fs::create_dir_all(path.join("config/xfer")).unwrap();
    fs::write(path.join("config/xfer/xfer.conf"), "xfer:debug-file: ${cwd}/global.log\n").unwrap();
    let script = path.join("commands.rc");
    fs::write(&script, "debug 2\nshow secret-one\nset xfer:debug-file '${cwd}/other-${pid}.log'\ndebug\nshow secret-two\nset xfer:debug-file missing/dir/log\ndebug\n").unwrap();
    let output = command(path).arg("-f").arg(&script).stderr(Stdio::piped()).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cannot open debug log"), "{stdout}");
    assert!(stdout.contains("other-"), "{stdout}");
    let initial = fs::read_to_string(path.join("global.log")).unwrap();
    assert!(initial.contains("TRACE dispatching command show"));
    let other = fs::read_dir(path).unwrap().filter_map(Result::ok).find(|e| e.file_name().to_string_lossy().starts_with("other-")).unwrap().path();
    let switched = fs::read_to_string(other).unwrap();
    assert!(switched.contains("TRACE dispatching command show"));
    assert!(!initial.contains("secret-one"));
    assert!(!switched.contains("secret-two"));
    // A second daemon can append to the configured shared file.
    fs::write(&script, "debug 2\nshow\n").unwrap();
    let output = command(path).arg("-f").arg(&script).output().unwrap();
    assert!(output.status.success());
    let appended = fs::read_to_string(path.join("global.log")).unwrap();
    assert!(appended.len() > initial.len());
}

#[test]
fn script_reports_missing_file_and_rejects_interactive_commands() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    fs::create_dir_all(path.join("home")).unwrap();
    let script = path.join("missing.rc");
    let output = command(path)
        .arg("-f")
        .arg(&script)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("missing.rc"));
    fs::write(&script, "bookmark edit\n").unwrap();
    let output = command(path)
        .arg("--file")
        .arg(&script)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not available in scripts"));
}
