//! Shared primitives for the xfer client/daemon split.

use std::{io, path::PathBuf};

/// Per-user directory shared by settings and bookmarks.
pub fn config_dir() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("xfer")
}

/// Private, per-user runtime directory used for daemon sockets and discovery files.
pub fn runtime_dir() -> io::Result<PathBuf> {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_STATE_HOME").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state")))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no XDG runtime/state directory or HOME",
            )
        })?;
    let path = base.join("xfer/instances");
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

pub mod core;

#[derive(Clone, Debug)]
pub struct Instance {
    pub id: String,
    pub pid: u32,
    pub socket: PathBuf,
}
pub fn instances() -> std::io::Result<Vec<Instance>> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(runtime_dir()?)? {
        let path = entry?.path();
        if path.extension().and_then(|v| v.to_str()) != Some("meta") {
            continue;
        }
        let text = match std::fs::read_to_string(&path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let mut id = None;
        let mut pid = None;
        let mut socket = None;
        for line in text.lines() {
            if let Some((k, v)) = line.split_once('=') {
                match k {
                    "id" => id = Some(v.into()),
                    "pid" => pid = v.parse().ok(),
                    "socket" => socket = Some(PathBuf::from(v)),
                    _ => {}
                }
            }
        }
        if let (Some(id), Some(pid), Some(socket)) = (id, pid, socket) {
            if socket.exists() {
                result.push(Instance { id, pid, socket });
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    result.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(result)
}
pub mod backend;
pub mod bookmarks;
pub mod cache;
pub mod commands;
pub mod credentials;
pub mod daemon_server;
pub mod error;
pub mod listing;
mod local_path;
pub mod local_shell;
pub mod s3;
pub mod sftp;
pub mod ssh_askpass;
pub mod transfer;

pub mod walker;
