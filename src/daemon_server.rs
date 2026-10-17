//! Local per-session daemon server.

// ------------------------------------------------------------------------------------------------

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use zeroize::{Zeroize, Zeroizing};

// ------------------------------------------------------------------------------------------------

/// State shared between the accept loop and per-connection worker threads.
struct Shared {
    attached: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    startup_claimed: Arc<AtomicBool>,
    daemon: Arc<Mutex<crate::core::Daemon>>,
}

/// Request prefixes answered from the completion cache or the local filesystem.
const COMPLETION_PREFIXES: [&str; 6] = [
    "COMPLETE_CD ",
    "COMPLETE_LS ",
    "COMPLETE_GET ",
    "COMPLETE_BOOKMARK ",
    "COMPLETE_LCD ",
    "COMPLETE_PUT ",
];

pub async fn main() -> Result<()> {
    let (listener, socket, metadata) = bind_socket()?;
    let shared = Shared {
        attached: Arc::new(AtomicBool::new(false)),
        shutdown: Arc::new(AtomicBool::new(false)),
        startup_claimed: Arc::new(AtomicBool::new(false)),
        daemon: Arc::new(Mutex::new(crate::core::Daemon::bootstrap()?)),
    };
    eprintln!("xferd listening on {}", socket.display());
    while !shared.shutdown.load(Ordering::Acquire) {
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let runtime = tokio::runtime::Handle::current();
        let shared = Shared {
            attached: shared.attached.clone(),
            shutdown: shared.shutdown.clone(),
            startup_claimed: shared.startup_claimed.clone(),
            daemon: shared.daemon.clone(),
        };
        thread::spawn(move || serve_connection(shared, stream, runtime));
    }
    let _ = fs::remove_file(&socket);
    let _ = fs::remove_file(&metadata);
    Ok(())
}

/// Bind the private per-session socket, publish the discovery metadata file, and
/// lock both down to the current user.
fn bind_socket() -> Result<(UnixListener, PathBuf, PathBuf)> {
    let socket = socket_path()?;
    if socket.exists() {
        fs::remove_file(&socket)?;
    }

    let listener =
        UnixListener::bind(&socket).with_context(|| format!("bind {}", socket.display()))?;

    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let metadata = socket.with_extension("meta");
    fs::write(
        &metadata,
        format!(
            "id={}\npid={}\nsocket={}\n",
            std::process::id(),
            std::process::id(),
            socket.display()
        ),
    )?;
    fs::set_permissions(&metadata, fs::Permissions::from_mode(0o600))?;
    Ok((listener, socket, metadata))
}

/// One worker thread per client connection: handshake, then serve requests until
/// detach, disconnect, or error. Releases the attach lease on every exit path.
fn serve_connection(shared: Shared, stream: UnixStream, runtime: tokio::runtime::Handle) {
    let _runtime_guard = runtime.enter();
    let mut reader = BufReader::new(stream.try_clone().expect("clone client socket"));
    let mut request = Zeroizing::new(String::new());
    let result = reader.read_line(&mut request);
    let mut writer = stream;
    if result.is_err() {
        return;
    }
    if request.trim() == "PROBE" {
        let _ = writer.write_all(if shared.attached.load(Ordering::Acquire) {
            b"BUSY\n"
        } else {
            b"READY\n"
        });
        return;
    }
    if request.trim() != "ATTACH" {
        let _ = writer.write_all(b"ERROR expected ATTACH\n");
        return;
    }
    if shared
        .attached
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        let _ = writer.write_all(b"BUSY\n");
        return;
    }
    let _ = writer.write_all(b"ATTACHED\n");
    let _ = writer.flush();
    // The lease belongs to this connection until it explicitly detaches or closes.
    loop {
        request.zeroize();
        match reader.read_line(&mut request) {
            Ok(0) | Err(_) => break,
            Ok(_) if request.trim() == "DETACH" => break,
            Ok(_) => handle_request(&shared, &mut request, &mut writer, &runtime),
        }
    }
    shared.attached.store(false, Ordering::Release);
    if !shared
        .daemon
        .lock()
        .expect("daemon state poisoned")
        .has_running_jobs()
    {
        shared.shutdown.store(true, Ordering::Release);
    }
}

fn handle_request(
    shared: &Shared,
    request: &mut Zeroizing<String>,
    writer: &mut UnixStream,
    runtime: &tokio::runtime::Handle,
) {
    if request.trim() == "STARTUP" {
        return handle_startup(shared, writer);
    }
    if request.starts_with("STARTUP_FILE ") {
        return handle_startup_file(shared, request, writer);
    }
    if request.trim() == "LOCAL_CWD" {
        return handle_local_cwd_query(shared, writer);
    }
    if request.starts_with("LOCAL_CWD ") {
        return handle_local_cwd_set(shared, request, writer);
    }
    if request.trim() == "SESSION" {
        return handle_session(shared, writer);
    }
    if COMPLETION_PREFIXES
        .iter()
        .any(|prefix| request.starts_with(prefix))
    {
        return handle_completion(shared, request, writer, runtime);
    }
    if request.starts_with("CONNECT_PASSWORD ") {
        return handle_connect_password(shared, request, writer);
    }
    if request.starts_with("COMMAND ") {
        return handle_command(shared, request, writer);
    }
    let _ = writer.write_all(b"ERROR unknown request\n");
}

fn handle_startup(shared: &Shared, writer: &mut UnixStream) {
    let response: &[u8] = if shared.startup_claimed.swap(true, Ordering::AcqRel) {
        b"STARTUP_DONE\n"
    } else {
        b"RUN_STARTUP\n"
    };
    let _ = writer.write_all(response);
    let _ = writer.flush();
}

fn handle_startup_file(shared: &Shared, request: &str, writer: &mut UnixStream) {
    match serde_json::from_str::<PathBuf>(request[13..].trim()) {
        Ok(path) => {
            shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .set_startup_file(path);
            let _ = writer.write_all(b"STARTUP_FILE_OK\n");
        }
        Err(_) => {
            let _ = writer.write_all(b"ERROR invalid startup file path\n");
        }
    }
    let _ = writer.flush();
}

fn handle_local_cwd_query(shared: &Shared, writer: &mut UnixStream) {
    let result = serde_json::to_string(
        &shared
            .daemon
            .lock()
            .expect("daemon state poisoned")
            .local_directory,
    );
    match result {
        Ok(path) => {
            let _ = writeln!(writer, "LOCAL_CWD {path}");
        }
        Err(error) => {
            let _ = writeln!(
                writer,
                "LOCAL_CWD_ERROR {}",
                serde_json::to_string(&error.to_string()).unwrap()
            );
        }
    }
    let _ = writer.flush();
}

fn handle_local_cwd_set(shared: &Shared, request: &str, writer: &mut UnixStream) {
    let result = serde_json::from_str::<String>(request[10..].trim())
        .map_err(anyhow::Error::from)
        .and_then(|path| {
            shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .set_local_directory(PathBuf::from(path))
        });
    match result {
        Ok(()) => {
            let _ = writer.write_all(b"LOCAL_CWD_OK\n");
        }
        Err(error) => {
            let _ = writeln!(
                writer,
                "LOCAL_CWD_ERROR {}",
                serde_json::to_string(&error.to_string()).unwrap()
            );
        }
    }
    let _ = writer.flush();
}

fn handle_session(shared: &Shared, writer: &mut UnixStream) {
    let info = shared
        .daemon
        .lock()
        .expect("daemon state poisoned")
        .session_info();
    let _ = writeln!(writer, "SESSION {}", serde_json::to_string(&info).unwrap());
    let _ = writer.flush();
}

fn handle_completion(
    shared: &Shared,
    request: &str,
    writer: &mut UnixStream,
    runtime: &tokio::runtime::Handle,
) {
    let result = match serde_json::from_str::<String>(request.split_once(' ').unwrap().1.trim()) {
        Ok(input)
            if request.starts_with("COMPLETE_LCD ") || request.starts_with("COMPLETE_PUT ") =>
        {
            let directory = shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .local_directory
                .clone();
            // Local filesystem only, independent of the backend and its
            // connection lock. Re-read each time, using fresh Local CWD.
            if request.starts_with("COMPLETE_PUT ") {
                crate::local_path::complete_files(&directory, &input)
            } else {
                crate::local_path::complete_directories(&directory, &input)
            }
        }
        Ok(input) => {
            // Do not hold the daemon-state mutex over network I/O.
            let daemon = shared.daemon.lock().expect("daemon state poisoned");
            if request.starts_with("COMPLETE_BOOKMARK ") {
                daemon.bookmark_completion(&input)
            } else {
                runtime.block_on(daemon.path_completion(input, request.starts_with("COMPLETE_CD ")))
            }
        }
        Err(_) => Err(anyhow::anyhow!("invalid completion request")),
    };
    match result {
        Ok(completion) => {
            let _ = writeln!(
                writer,
                "COMPLETION {}",
                serde_json::to_string(&completion).unwrap()
            );
        }
        Err(error) => {
            let _ = writeln!(
                writer,
                "COMPLETION_ERROR {}",
                serde_json::to_string(&error.to_string()).unwrap()
            );
        }
    }
    let _ = writer.flush();
}

fn handle_connect_password(
    shared: &Shared,
    request: &mut Zeroizing<String>,
    writer: &mut UnixStream,
) {
    let response =
        match serde_json::from_str::<crate::credentials::PasswordConnect>(request[17..].trim()) {
            Ok(credentials) => shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .connect_command(&credentials.target, Some(credentials.password)),
            // Parser errors may contain the password: never include them.
            Err(_) => vec!["invalid password connection request".into()],
        };
    request.zeroize();
    for line in response {
        let _ = writeln!(writer, "OUTPUT {line}");
    }
    let _ = writer.write_all(b"DONE\n");
    let _ = writer.flush();
}

fn handle_command(shared: &Shared, request: &str, writer: &mut UnixStream) {
    let response = shared
        .daemon
        .lock()
        .expect("daemon state poisoned")
        .command(request[8..].trim());
    for line in response {
        let _ = writeln!(writer, "OUTPUT {line}");
    }
    let _ = writer.write_all(b"DONE\n");
    let _ = writer.flush();
}

fn socket_path() -> Result<PathBuf> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() == Some(std::ffi::OsStr::new("--socket")) {
        return args
            .next()
            .map(PathBuf::from)
            .context("--socket requires a path");
    }
    let dir = crate::runtime_dir()?;
    Ok(dir.join(format!("{}.sock", std::process::id())))
}
