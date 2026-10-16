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
use zeroize::{Zeroize, Zeroizing};

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
    let startup_claimed = Arc::new(AtomicBool::new(false));
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
        let startup_claimed = startup_claimed.clone();
        let shutdown = shutdown.clone();
        let daemon = daemon.clone();
        let runtime = tokio::runtime::Handle::current();
        thread::spawn(move || {
            let _runtime_guard = runtime.enter();
            let mut reader = BufReader::new(stream.try_clone().expect("clone client socket"));
            let mut request = Zeroizing::new(String::new());
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
                request.zeroize();
                match reader.read_line(&mut request) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if request.trim() == "DETACH" => break,
                    Ok(_) if request.trim() == "STARTUP" => {
                        let response: &[u8] = if startup_claimed.swap(true, Ordering::AcqRel) {
                            b"STARTUP_DONE\n"
                        } else {
                            b"RUN_STARTUP\n"
                        };
                        let _ = writer.write_all(response);
                        let _ = writer.flush();
                    }
                    Ok(_) if request.starts_with("STARTUP_FILE ") => {
                        match serde_json::from_str::<PathBuf>(request[13..].trim()) {
                            Ok(path) => {
                                daemon
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
                    Ok(_) if request.starts_with("LOCAL_CWD ") => {
                        let result = serde_json::from_str::<String>(request[10..].trim())
                            .map_err(anyhow::Error::from)
                            .and_then(|path| {
                                daemon
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
                    Ok(_) if request.trim() == "SESSION" => {
                        let info = daemon.lock().expect("daemon state poisoned").session_info();
                        let _ =
                            writeln!(writer, "SESSION {}", serde_json::to_string(&info).unwrap());
                        let _ = writer.flush();
                    }
                    Ok(_)
                        if request.starts_with("COMPLETE_CD ")
                            || request.starts_with("COMPLETE_LS ")
                            || request.starts_with("COMPLETE_GET ")
                            || request.starts_with("COMPLETE_BOOKMARK ") =>
                    {
                        let result = match serde_json::from_str::<String>(
                            request.split_once(' ').unwrap().1.trim(),
                        ) {
                            Ok(input) => {
                                // Do not hold the daemon-state mutex over network I/O.
                                let daemon = daemon.lock().expect("daemon state poisoned");
                                if request.starts_with("COMPLETE_BOOKMARK ") {
                                    daemon.bookmark_completion(&input)
                                } else {
                                    runtime.block_on(daemon.path_completion(
                                        input,
                                        request.starts_with("COMPLETE_CD "),
                                    ))
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
                    Ok(_) if request.starts_with("CONNECT_PASSWORD ") => {
                        let response = match serde_json::from_str::<
                            crate::credentials::PasswordConnect,
                        >(request[17..].trim())
                        {
                            Ok(credentials) => daemon
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
