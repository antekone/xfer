//! Daemon diagnostics. Arguments, job output and error details can contain secrets.
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const DEFAULT_PATH: &str = "${cwd}/xfer-debug-${pid}.log";

struct Destination {
    path: PathBuf,
    file: Option<fs::File>,
}

pub(crate) struct DebugLog {
    destination: Mutex<Destination>,
    level: AtomicU8,
}

// Expand only our own variables; never invoke a shell or expand the environment.
fn expand_path(template: &str, cwd: &Path) -> io::Result<PathBuf> {
    let mut result = OsString::new();
    let mut rest = template;
    while !rest.is_empty() {
        if let Some(tail) = rest
            .strip_prefix("${pid}")
            .or_else(|| rest.strip_prefix("$pid"))
        {
            result.push(std::process::id().to_string());
            rest = tail;
        } else if let Some(tail) = rest
            .strip_prefix("${cwd}")
            .or_else(|| rest.strip_prefix("$cwd"))
        {
            result.push(cwd.as_os_str());
            rest = tail;
        } else {
            let ch = rest.chars().next().unwrap();
            result.push(ch.to_string());
            rest = &rest[ch.len_utf8()..];
        }
    }
    if result.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "debug filename cannot be empty",
        ));
    }
    let path = PathBuf::from(result);
    Ok(if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    })
}

// Append allows multiple daemons to share a file. Refuse symlinks,
// non-regular files, other owners and public files, including on lazy open.
fn open_file(path: &Path, create: bool) -> io::Result<fs::File> {
    let file = OpenOptions::new()
        .append(true)
        .create(create)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "debug log must be a private regular file owned by the current user",
        ));
    }
    Ok(file)
}

fn prepare_destination(template: &str, cwd: &Path) -> io::Result<Destination> {
    let path = expand_path(template, cwd)?;
    match open_file(&path, false) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // Do not create the file yet, but reject nonexistent/non-directory
            // parents when applying the setting. Opening is checked again at use.
            if !fs::metadata(path.parent().unwrap_or(cwd))?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "debug log parent is not a directory",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(Destination { path, file: None })
}

impl DebugLog {
    pub(crate) fn create(template: &str, cwd: &Path) -> io::Result<Self> {
        Ok(Self {
            destination: Mutex::new(prepare_destination(template, cwd)?),
            level: AtomicU8::new(0),
        })
    }

    pub(crate) fn set_path(&self, template: &str, cwd: &Path) -> io::Result<()> {
        let destination = prepare_destination(template, cwd)?;
        *self
            .destination
            .lock()
            .map_err(|_| io::Error::other("debug log lock poisoned"))? = destination;
        Ok(())
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.destination
            .lock()
            .expect("debug log lock poisoned")
            .path
            .clone()
    }
    pub(crate) fn level(&self) -> u8 {
        self.level.load(Ordering::Relaxed)
    }
    pub(crate) fn set_level(&self, level: u8) {
        self.level.store(level, Ordering::Relaxed);
    }

    pub(crate) fn log(&self, level: u8, label: &str, message: &str) {
        if self.level() < level {
            return;
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        if let Ok(mut destination) = self.destination.lock() {
            let line = format!(
                "{}.{:03} pid={} {} {}\n",
                timestamp.as_secs(),
                timestamp.subsec_millis(),
                std::process::id(),
                label,
                message
            );
            if destination.file.is_none() {
                destination.file = open_file(&destination.path, true).ok();
            }
            if let Some(file) = &mut destination.file {
                let _ = file.write_all(line.as_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[test]
    fn private_file_and_level_filtering() {
        let dir = tempfile::tempdir().unwrap();
        let log = DebugLog::create(DEFAULT_PATH, dir.path()).unwrap();
        assert_eq!(
            log.path(),
            dir.path()
                .join(format!("xfer-debug-{}.log", std::process::id()))
        );
        assert!(!log.path().exists());
        log.log(1, "DEBUG", "hidden");
        assert!(!log.path().exists());
        log.log(0, "WARN", "warning");
        assert_eq!(
            fs::metadata(log.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        log.set_level(2);
        log.log(2, "TRACE", "visible");
        let contents = fs::read_to_string(log.path()).unwrap();
        assert!(contents.contains("warning"));
        assert!(!contents.contains("hidden"));
        assert!(contents.contains("visible"));
    }

    #[test]
    fn lazy_open_rechecks_safety_after_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, "untouched").unwrap();
        let log = DebugLog::create("log", dir.path()).unwrap();
        symlink(&target, log.path()).unwrap();
        log.log(0, "WARN", "unsafe");
        assert_eq!(fs::read_to_string(target).unwrap(), "untouched");
    }

    #[test]
    fn expansion_switch_and_file_safety() {
        let dir = tempfile::tempdir().unwrap();
        let log = DebugLog::create("$cwd/log-$pid", dir.path()).unwrap();
        let first = log.path();
        log.log(0, "WARN", "first");
        log.set_path("shared.log", dir.path()).unwrap();
        assert!(!dir.path().join("shared.log").exists());
        log.log(0, "WARN", "shared");
        assert!(fs::read_to_string(dir.path().join("shared.log"))
            .unwrap()
            .contains("shared"));
        symlink(&first, dir.path().join("link")).unwrap();
        assert!(log.set_path("link", dir.path()).is_err());
        assert_eq!(log.path(), dir.path().join("shared.log"));
        fs::set_permissions(&first, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(log.set_path(first.to_str().unwrap(), dir.path()).is_err());
        assert!(log.set_path("", dir.path()).is_err());
    }
}
