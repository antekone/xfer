//! Local per-session daemon server.

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use anyhow::{Context, Result};

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

pub async fn run() -> Result<()> {
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

    let attached = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(AtomicBool::new(false));
    let daemon = Arc::new(Mutex::new(crate::core::Daemon::bootstrap()?));
    eprintln!("xferd listening on {}", socket.display());
    while !shutdown.load(Ordering::Acquire) {
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let lease = attached.clone();
        let shutdown = shutdown.clone();
        let daemon = daemon.clone();
        let runtime = tokio::runtime::Handle::current();
        thread::spawn(move || {
            let _runtime_guard = runtime.enter();
            let mut reader = BufReader::new(stream.try_clone().expect("clone client socket"));
            let mut request = String::new();
            let result = reader.read_line(&mut request);
            let mut writer = stream;
            if result.is_err() {
                return;
            }
            if request.trim() == "PROBE" {
                let _ = writer.write_all(if lease.load(Ordering::Acquire) {
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
            if lease
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
                request.clear();
                match reader.read_line(&mut request) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if request.trim() == "DETACH" => break,
                    Ok(_) if request.starts_with("COMMAND ") => {
                        let response = daemon
                            .lock()
                            .expect("daemon state poisoned")
                            .command(request[8..].trim());
                        for line in response {
                            let _ = writeln!(writer, "OUTPUT {line}");
                        }
                        let _ = writer.write_all(b"DONE\n");
                        let _ = writer.flush();
                    }
                    Ok(_) => {
                        let _ = writer.write_all(b"ERROR unknown request\n");
                    }
                }
            }
            lease.store(false, Ordering::Release);
            if !daemon
                .lock()
                .expect("daemon state poisoned")
                .has_running_jobs()
            {
                shutdown.store(true, Ordering::Release);
            }
        });
    }
    let _ = fs::remove_file(&socket);
    let _ = fs::remove_file(&metadata);
    Ok(())
}
