//! SFTP v3 over the installed OpenSSH client. No remote shell commands are used.
use crate::{
    backend::{Backend, ConnectOptions, Connection},
    core::{SettingValue, Settings},
    error::NonRetryableError,
    listing::{Content, DirectoryInfo, FileInfo, ListingSink, UnixMetadata},
    ssh_askpass::Askpass,
    transfer::{DownloadSink, RemoteFile},
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use openssh_sftp_client::{metadata::MetaData, Sftp as Client, SftpOptions};
use percent_encoding::percent_decode_str;
use std::{
    future::Future,
    io::SeekFrom,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    process::{Child, Command},
    task::JoinHandle,
};

const IO_TIMEOUT: Duration = Duration::from_secs(30);
pub struct Sftp;

fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}

#[derive(Debug, PartialEq)]
struct Target {
    host: String,
    user: Option<String>,
    port: Option<u16>,
    directory: String,
}
impl Target {
    fn parse(value: &str) -> Result<Self> {
        let url = url::Url::parse(value).map_err(|_| permanent("invalid SFTP URL"))?;
        if url.scheme() != "sftp"
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(permanent("expected sftp://[user@]host[:port][/directory]; passwords belong in the interactive prompt"));
        }
        let host = match url.host() {
            Some(url::Host::Domain(host)) => host.to_owned(),
            Some(url::Host::Ipv4(host)) => host.to_string(),
            Some(url::Host::Ipv6(host)) => host.to_string(),
            None => return Err(permanent("SFTP URL requires a host")),
        };
        if host.starts_with('-')
            || host.chars().any(|c| c.is_whitespace() || c.is_control())
            || url.port() == Some(0)
        {
            return Err(permanent("invalid SFTP host or port"));
        }
        let decode = |value: &str| -> Result<String> {
            let value = percent_decode_str(value)
                .decode_utf8()
                .map_err(|_| permanent("SFTP URL must be valid UTF-8"))?
                .into_owned();
            if value.chars().any(char::is_control) {
                return Err(permanent("control characters are not allowed in SFTP URLs"));
            }
            Ok(value)
        };
        let user = if url.username().is_empty() {
            None
        } else {
            Some(decode(url.username())?)
        };
        let directory = if url.path().is_empty() {
            ".".into()
        } else {
            decode(url.path())?
        };
        Ok(Self {
            host,
            user,
            port: url.port(),
            directory,
        })
    }

    fn initial_directory(&self, expand_path: bool) -> String {
        // The slash separates the URL authority from a remote-home path; it is
        // not an absolute /~ directory on the server. Other tildes stay literal.
        let home_path = self
            .directory
            .strip_prefix('/')
            .filter(|path| *path == "~" || path.starts_with("~/"));
        match home_path {
            Some(path) if expand_path => path.to_owned(),
            // Standard SFTP has no HOME/environment query. REALPATH of a
            // relative path resolves against the server's initial directory.
            Some(path) => format!(".{}", &path[1..]),
            None => self.directory.clone(),
        }
    }

    fn description(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let user = self
            .user
            .as_ref()
            .map(|user| format!("{user}@"))
            .unwrap_or_default();
        let port = self.port.map(|port| format!(":{port}")).unwrap_or_default();
        format!("{user}{host}{port}")
    }

    fn command(&self, timeout: u64, password: bool) -> Command {
        let mut command = Command::new("ssh");
        command
            .args([
                "-T",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                "NumberOfPasswordPrompts=1",
                "-o",
                "KbdInteractiveAuthentication=no",
                "-o",
                "PermitLocalCommand=no",
                "-o",
                "ClearAllForwardings=yes",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
            ])
            .arg("-o")
            .arg(format!("ConnectTimeout={timeout}"))
            .arg("-o")
            .arg(if password {
                "BatchMode=no"
            } else {
                "BatchMode=yes"
            });
        if let Some(user) = &self.user {
            command.arg("-l").arg(user);
        }
        if let Some(port) = self.port {
            command.arg("-p").arg(port.to_string());
        }
        command
            .arg("-s")
            .arg("--")
            .arg(&self.host)
            .arg("sftp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Never let an inherited desktop askpass prompt consume credentials.
        command
            .env_remove(crate::ssh_askpass::SOCKET_ENV)
            .env("SSH_ASKPASS_REQUIRE", "never");
        command
    }
}

struct SshProcess {
    child: Child,
    diagnostics: Arc<Mutex<Vec<u8>>>,
    stderr_task: JoinHandle<()>,
}
impl SshProcess {
    fn spawn(command: &mut Command) -> Result<Self> {
        let mut child = command.spawn().context("start installed ssh client")?;
        let mut stderr = child.stderr.take().expect("piped SSH stderr");
        let diagnostics = Arc::new(Mutex::new(Vec::new()));
        let saved = diagnostics.clone();
        let stderr_task = tokio::spawn(async move {
            let mut buffer = [0; 1024];
            while let Ok(n) = stderr.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                let mut saved = saved.lock().unwrap();
                let remaining = 8192usize.saturating_sub(saved.len());
                saved.extend_from_slice(&buffer[..n.min(remaining)]);
            }
        });
        Ok(Self {
            child,
            diagnostics,
            stderr_task,
        })
    }
    async fn failure_status(&mut self, timed_out: bool) -> Option<ExitStatus> {
        // Preserve SSH's actual exit status instead of immediately killing it.
        let status = if timed_out {
            self.child.try_wait().ok().flatten()
        } else {
            tokio::time::timeout(Duration::from_millis(200), self.child.wait())
                .await
                .ok()
                .and_then(Result::ok)
        };
        if status.is_none() {
            let _ = self.child.start_kill();
            let _ = tokio::time::timeout(Duration::from_millis(200), self.child.wait()).await;
        }
        // Reaping the process does not mean its stderr reader has drained the
        // pipe. Await that reader before formatting the error, with a bound for
        // proxy processes which might still hold the pipe open.
        let _ = tokio::time::timeout(Duration::from_millis(200), &mut self.stderr_task).await;
        status
    }

    fn diagnostic(&self) -> String {
        String::from_utf8_lossy(&self.diagnostics.lock().unwrap())
            .trim()
            .to_owned()
    }
}
impl Drop for SshProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.stderr_task.abort();
    }
}

#[async_trait]
impl Backend for Sftp {
    fn protocol(&self) -> &'static str {
        "sftp"
    }
    fn register_settings(&self, settings: &mut Settings) {
        settings.register(
            "sftp:connect-timeout",
            SettingValue::Int(30),
            "SSH connection/authentication timeout (seconds)",
        );
    }
    async fn connect(&self, options: ConnectOptions) -> Result<Box<dyn Connection>> {
        let target = Target::parse(
            options
                .bookmark
                .as_ref()
                .map(|b| b.url.as_str())
                .unwrap_or(&options.target),
        )?;
        let timeout = match options
            .settings
            .values
            .get("sftp:connect-timeout")
            .map(|s| &s.value)
        {
            Some(SettingValue::Int(value)) if *value > 0 => *value as u64,
            _ => return Err(permanent("sftp:connect-timeout must be positive")),
        };
        let askpass = options
            .password
            .clone()
            .map(Askpass::new)
            .transpose()
            .map_err(|error| {
                permanent(format!(
                    "cannot initialize private SSH password helper: {error:#}"
                ))
            })?;
        let mut command = target.command(timeout, askpass.is_some());
        if let Some(askpass) = &askpass {
            askpass.configure(&mut command)?;
        }
        let mut process = SshProcess::spawn(&mut command).map_err(|error| {
            permanent(format!("connect to {}: {error:#}", target.description()))
        })?;
        let stdin = process.child.stdin.take().unwrap();
        let stdout = process.child.stdout.take().unwrap();
        let mut stage = "start SFTP subsystem";
        let result = tokio::time::timeout(Duration::from_secs(timeout), async {
            let sftp = Client::new(stdin, stdout, SftpOptions::new())
                .await
                .context(stage)?;
            stage = "resolve initial remote directory";
            let initial_directory = target.initial_directory(sftp.support_expand_path());
            let home = sftp
                .fs()
                .canonicalize(&initial_directory)
                .await
                .with_context(|| format!("{stage} {:?}", target.directory))?;
            stage = "stat initial remote directory";
            let metadata = sftp
                .fs()
                .metadata(&home)
                .await
                .with_context(|| format!("{stage} {}", home.display()))?;
            Ok::<_, anyhow::Error>((sftp, home, metadata))
        })
        .await;
        match result {
            Ok(Ok((sftp, home, metadata))) => {
                if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
                    return Err(permanent(format!(
                        "SFTP initial path {} on {} is not a directory",
                        home.display(),
                        target.description()
                    )));
                }
                let home = remote_path(&home)?;
                drop(askpass); // The authenticated session needs no password retained.
                Ok(Box::new(SftpConnection {
                    sftp,
                    process,
                    cwd: home.clone(),
                    home,
                }))
            }
            result => {
                let timed_out = result.is_err();
                let error = match result {
                    Ok(Err(error)) => error,
                    Err(_) => anyhow::anyhow!("timed out after {timeout}s while trying to {stage}"),
                    Ok(Ok(_)) => unreachable!(),
                };
                let status = process.failure_status(timed_out).await;
                let diagnostic = process.diagnostic();
                let message = connection_error(
                    &target,
                    &error,
                    status,
                    &diagnostic,
                    options.password.as_ref(),
                );
                if timed_out || transient_ssh_error(&diagnostic) {
                    bail!("{message}");
                }
                // Authentication/host-key/configuration failures must not retry forever
                // with the same password. A new open command obtains a new password.
                Err(permanent(message))
            }
        }
    }
}

fn connection_error(
    target: &Target,
    error: &anyhow::Error,
    status: Option<ExitStatus>,
    diagnostic: &str,
    password: Option<&crate::credentials::Password>,
) -> String {
    let mut message = format!(
        "SSH/SFTP connection to {} failed: {error:#}",
        target.description()
    );
    if let Some(status) = status {
        message.push_str(&format!("; SSH {status}"));
    }
    if !diagnostic.trim().is_empty() {
        message.push_str(&format!("; SSH stderr: {}", diagnostic.trim()));
    }
    // Redact the entire cause chain and stderr BEFORE bounding the output, so
    // truncation cannot expose a password prefix. Never emit terminal controls.
    if let Some(password) = password.filter(|p| !p.expose().is_empty()) {
        message = message.replace(password.expose(), "[redacted]");
    }
    message
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .take(4096)
        .collect()
}

fn transient_ssh_error(diagnostic: &str) -> bool {
    let diagnostic = diagnostic.to_ascii_lowercase();
    [
        "connection refused",
        "connection timed out",
        "connection reset",
        "connection closed",
        "no route to host",
        "network is unreachable",
        "could not resolve hostname",
    ]
    .iter()
    .any(|message| diagnostic.contains(message))
}

fn remote_path(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .ok_or_else(|| permanent("non-UTF-8 remote paths are not supported"))?;
    if !path.starts_with('/') {
        return Err(permanent("SFTP server returned a non-absolute path"));
    }
    Ok(directory(&normalize("/", path)))
}
fn normalize(cwd: &str, path: &str) -> String {
    let mut parts = if path.starts_with('/') {
        Vec::new()
    } else {
        cwd.split('/').filter(|p| !p.is_empty()).collect::<Vec<_>>()
    };
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            _ => parts.push(part),
        }
    }
    format!("/{}", parts.join("/"))
}
fn directory(path: &str) -> String {
    if path.ends_with('/') {
        path.to_owned()
    } else {
        format!("{path}/")
    }
}
fn directory_input(path: &str) -> bool {
    path.is_empty() || path.ends_with('/') || matches!(path.rsplit('/').next(), Some("." | ".."))
}
async fn operation<T>(
    future: impl Future<Output = Result<T, openssh_sftp_client::Error>>,
) -> Result<T> {
    use openssh_sftp_client::{error::SftpErrorKind, Error};
    let error = match tokio::time::timeout(IO_TIMEOUT, future).await {
        Ok(Ok(value)) => return Ok(value),
        Ok(Err(error)) => error,
        Err(_) => return Err(permanent("SFTP operation timed out; reconnect with open")),
    };
    let message = match error {
        Error::SftpError(SftpErrorKind::NoSuchFile, _) => "no such remote file or directory".into(),
        Error::SftpError(SftpErrorKind::PermDenied, _) => "remote permission denied".into(),
        Error::SftpError(kind, _) => format!("remote SFTP operation failed ({kind:?})"),
        _ => "SFTP transport failed; reconnect with open".into(),
    };
    Err(permanent(message))
}
fn record(name: String, metadata: &MetaData, directory: bool) -> Content {
    let modified = metadata
        .modified()
        .and_then(|t| chrono::DateTime::from_timestamp(t.into_raw() as i64, 0));
    let unix = Some(UnixMetadata {
        mode: metadata.permissions().map(|permissions| {
            permissions.as_raw().bits()
                | metadata
                    .file_type()
                    .map(|kind| kind.as_raw() as u32)
                    .unwrap_or(0)
        }),
        uid: metadata.uid(),
        gid: metadata.gid(),
    });
    if directory {
        Content::Directory(DirectoryInfo {
            name,
            modified,
            size: metadata.len(),
            unix,
        })
    } else {
        Content::File(FileInfo {
            name,
            size: metadata.len(),
            modified,
            unix,
        })
    }
}
fn remote_file(metadata: &MetaData) -> Result<RemoteFile> {
    if !metadata.file_type().is_some_and(|kind| kind.is_file()) {
        return Err(permanent("get expects a regular remote file"));
    }
    Ok(RemoteFile {
        size: metadata
            .len()
            .ok_or_else(|| permanent("SFTP server did not report file size"))?,
        revision: metadata.modified().map(|time| time.into_raw().to_string()),
    })
}
fn unchanged(metadata: &MetaData, expected: &RemoteFile) -> Result<()> {
    let current = remote_file(metadata)?;
    if current.size != expected.size || current.revision != expected.revision {
        return Err(permanent("remote file changed; partial file was retained"));
    }
    Ok(())
}

struct SftpConnection {
    sftp: Client,
    process: SshProcess,
    cwd: String,
    home: String,
}
#[async_trait]
impl Connection for SftpConnection {
    fn protocol(&self) -> &'static str {
        "sftp"
    }
    fn pwd(&self) -> Result<String> {
        Ok(self.cwd.clone())
    }
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        Ok(match path {
            None => self.home.clone(),
            Some(path) => directory(&normalize(&self.cwd, path)),
        })
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        let resolved = normalize(&self.cwd, path);
        Ok(if directory_input(path) {
            directory(&resolved)
        } else {
            resolved
        })
    }
    async fn change_directory(&mut self, path: &str) -> Result<()> {
        let path = self.resolve_directory(Some(path))?;
        let canonical = operation(self.sftp.fs().canonicalize(&path)).await?;
        let metadata = operation(self.sftp.fs().metadata(&canonical)).await?;
        if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
            return Err(permanent("not a remote directory"));
        }
        self.cwd = remote_path(&canonical)?;
        Ok(())
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let path = self.resolve_listing_path(path)?;
        let metadata = operation(self.sftp.fs().metadata(&path)).await?;
        if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
            if directory_input(&path) {
                return Err(permanent("not a remote directory"));
            }
            let name = path.rsplit('/').next().unwrap_or(&path).to_owned();
            output.emit(record(name, &metadata, false))?;
            return Ok(1);
        }
        let dir = operation(self.sftp.fs().open_dir(&path)).await?;
        let mut entries = Box::pin(dir.read_dir());
        let mut count = 0;
        loop {
            let entry = tokio::time::timeout(IO_TIMEOUT, entries.next())
                .await
                .map_err(|_| permanent("SFTP directory read timed out; reconnect with open"))?;
            let Some(entry) = entry else {
                break;
            };
            let entry = operation(async { entry }).await?;
            let name = entry
                .filename()
                .to_str()
                .ok_or_else(|| permanent("non-UTF-8 remote filenames are not supported"))?;
            if matches!(name, "." | "..") {
                continue;
            }
            if name.contains('/') {
                return Err(permanent("SFTP server returned an invalid filename"));
            }
            let metadata = entry.metadata();
            let mut directory = metadata.file_type().is_some_and(|kind| kind.is_dir());
            // Follow symlinks only to classify directory completion. Keep the
            // link's own mode, owner, size and mtime in the listing record.
            if metadata.file_type().is_some_and(|kind| kind.is_symlink()) {
                if let Ok(followed) = operation(
                    self.sftp
                        .fs()
                        .metadata(format!("{}/{name}", path.trim_end_matches('/'))),
                )
                .await
                {
                    directory = followed.file_type().is_some_and(|kind| kind.is_dir());
                }
            }
            output.emit(record(name.to_owned(), &metadata, directory))?;
            count += 1;
        }
        Ok(count)
    }
    async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
        let path = self.resolve_listing_path(path)?;
        let metadata = operation(self.sftp.fs().metadata(path)).await?;
        remote_file(&metadata)
    }
    async fn download(
        &self,
        path: &str,
        offset: u64,
        expected: &RemoteFile,
        sink: &mut dyn DownloadSink,
    ) -> Result<()> {
        if offset > expected.size {
            return Err(permanent("resume offset exceeds remote size"));
        }
        let path = self.resolve_listing_path(path)?;
        let mut file = operation(self.sftp.open(path)).await?;
        unchanged(&operation(file.metadata()).await?, expected)?;
        let mut position = offset;
        while position < expected.size {
            // Explicit offsets also handle servers returning short reads.
            file.seek(SeekFrom::Start(position)).await?;
            let length = (expected.size - position).min(64 * 1024) as u32;
            let bytes = operation(file.read(length, Default::default()))
                .await?
                .filter(|bytes| !bytes.is_empty())
                .ok_or_else(|| permanent("remote file ended early; partial file was retained"))?;
            if bytes.len() as u64 > expected.size - position {
                return Err(permanent("SFTP server returned too much data"));
            }
            sink.write(&bytes)?;
            position += bytes.len() as u64;
        }
        unchanged(&operation(file.metadata()).await?, expected)?;
        operation(file.close()).await?;
        Ok(())
    }
    async fn disconnect(self: Box<Self>) -> Result<()> {
        let Self {
            sftp, mut process, ..
        } = *self;
        let result = tokio::time::timeout(Duration::from_secs(2), sftp.close()).await;
        let _ = process.child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), process.child.wait()).await;
        match result {
            Ok(result) => result.map_err(|_| permanent("SFTP close failed")),
            Err(_) => Err(permanent("SFTP close timed out")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssh_sftp_client::metadata::{MetaDataBuilder, Permissions, RawPermissions};

    #[test]
    fn urls_support_ssh_aliases_users_ports_ipv6_and_escaped_paths() {
        assert_eq!(
            Target::parse("sftp://my-alias").unwrap(),
            Target {
                host: "my-alias".into(),
                user: None,
                port: None,
                directory: ".".into(),
            }
        );
        assert_eq!(
            Target::parse("sftp://user%20name@[::1]:2222/path%20with%20spaces").unwrap(),
            Target {
                host: "::1".into(),
                user: Some("user name".into()),
                port: Some(2222),
                directory: "/path with spaces".into(),
            }
        );
        for url in [
            "https://host",
            "sftp://host:0",
            "sftp://-evil",
            "sftp://user:secret@host",
            "sftp://host/?secret=1",
            "sftp://host/#secret",
            "sftp://host/%00",
            "sftp://user%0a@host",
        ] {
            assert!(Target::parse(url).is_err(), "accepted {url}");
        }
    }

    #[test]
    fn url_tilde_paths_use_remote_expansion_or_login_directory_fallback() {
        for (url, expanded, fallback) in [
            ("sftp://seedhost/~", "~", "."),
            ("sftp://seedhost/~/", "~/", "./"),
            ("sftp://seedhost/~/downloads", "~/downloads", "./downloads"),
            (
                "sftp://seedhost/%7E/downloads",
                "~/downloads",
                "./downloads",
            ),
            ("sftp://seedhost", ".", "."),
            ("sftp://seedhost/", "/", "/"),
            ("sftp://seedhost/~other", "/~other", "/~other"),
            (
                "sftp://seedhost/path/~/file",
                "/path/~/file",
                "/path/~/file",
            ),
            ("sftp://seedhost/~photos", "/~photos", "/~photos"),
        ] {
            let target = Target::parse(url).unwrap();
            assert_eq!(target.initial_directory(true), expanded, "{url}");
            assert_eq!(target.initial_directory(false), fallback, "{url}");
        }
    }

    #[test]
    fn ssh_command_is_subsystem_only_and_verifies_host_keys() {
        let target = Target::parse("sftp://user@example.org:2222").unwrap();
        let command = target.command(5, false);
        let command = command.as_std();
        let args = command
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect::<Vec<_>>();
        assert!(args.contains(&"StrictHostKeyChecking=yes"));
        assert!(args.contains(&"BatchMode=yes"));
        assert!(args.contains(&"ConnectTimeout=5"));
        assert!(args.ends_with(&["-s", "--", "example.org", "sftp"]));
        assert!(args.windows(2).any(|args| args == ["-l", "user"]));
        assert!(args.windows(2).any(|args| args == ["-p", "2222"]));
        assert!(target
            .command(5, true)
            .as_std()
            .get_args()
            .any(|arg| arg == "BatchMode=no"));
    }

    #[test]
    fn paths_normalize_parents_without_escaping_root() {
        assert_eq!(
            normalize("/home/user/", "../other/./photos"),
            "/home/other/photos"
        );
        assert_eq!(normalize("/home/user/", "/tmp/../root"), "/root");
        assert_eq!(normalize("/home/user/", "../../../.."), "/");
        assert_eq!(directory(&normalize("/", "")), "/");
        assert!(directory_input("photos/.."));
        assert!(!directory_input("photo.jpg"));
    }

    fn metadata(size: u64, modified: u32) -> MetaData {
        let time = openssh_sftp_client::UnixTimeStamp::from_raw(modified).unwrap();
        MetaDataBuilder::new()
            .permissions(Permissions::from_raw(RawPermissions::from_bits_retain(
                0o100644,
            )))
            .len(size)
            .time(time, time)
            .create()
    }

    #[test]
    fn listing_records_preserve_posix_metadata_and_symlink_identity() {
        let time = openssh_sftp_client::UnixTimeStamp::from_raw(1234567890).unwrap();
        let metadata = MetaDataBuilder::new()
            .permissions(Permissions::from_raw(RawPermissions::from_bits_retain(
                0o100640,
            )))
            .id((1234, 5678))
            .len(200000)
            .time(time, time)
            .create();
        let Content::File(file) = record("file.txt".into(), &metadata, false) else {
            panic!("expected file");
        };
        assert_eq!(file.size, Some(200000));
        assert_eq!(file.modified.unwrap().timestamp(), 1234567890);
        assert_eq!(
            file.unix,
            Some(UnixMetadata {
                mode: Some(0o100640),
                uid: Some(1234),
                gid: Some(5678)
            })
        );

        for (mode, size) in [(0o040755, 4096), (0o120777, 6)] {
            let metadata = MetaDataBuilder::new()
                .permissions(Permissions::from_raw(RawPermissions::from_bits_retain(
                    mode,
                )))
                .id((1234, 5678))
                .len(size)
                .time(time, time)
                .create();
            let Content::Directory(directory) = record("photos".into(), &metadata, true) else {
                panic!("expected directory");
            };
            assert_eq!(directory.size, Some(size));
            assert_eq!(directory.unix.unwrap().mode, Some(mode));
            assert_eq!(directory.modified.unwrap().timestamp(), 1234567890);
        }
    }

    #[test]
    fn downloads_reject_size_or_mtime_changes_and_non_regular_files() {
        let expected = remote_file(&metadata(100, 10)).unwrap();
        unchanged(&metadata(100, 10), &expected).unwrap();
        assert!(unchanged(&metadata(101, 10), &expected)
            .unwrap_err()
            .is::<NonRetryableError>());
        assert!(unchanged(&metadata(100, 11), &expected).is_err());
        let directory = MetaDataBuilder::new()
            .permissions(Permissions::from_raw(RawPermissions::from_bits_retain(
                0o040755,
            )))
            .create();
        assert!(remote_file(&directory).is_err());
    }

    #[test]
    fn connection_errors_include_causes_and_redact_all_diagnostic_sources() {
        use std::os::unix::process::ExitStatusExt;
        let target = Target::parse("sftp://user@[::1]:2222").unwrap();
        let error = anyhow::anyhow!("underlying failure: secret\nnext line")
            .context("start SFTP subsystem");
        let password = crate::credentials::Password::new("secret".into());
        let message = connection_error(
            &target,
            &error,
            Some(ExitStatus::from_raw(255 << 8)),
            "Permission denied; credential=secret\r\n",
            Some(&password),
        );
        assert!(message.contains("user@[::1]:2222"));
        assert!(message.contains("start SFTP subsystem: underlying failure:"));
        assert!(message.contains("SSH exit status: 255"));
        assert!(message.contains("SSH stderr: Permission denied"));
        assert_eq!(message.matches("[redacted]").count(), 2);
        assert!(!message.contains("secret"));
        assert!(!message.chars().any(char::is_control));
    }

    #[test]
    fn connection_errors_redact_before_truncating() {
        let target = Target::parse("sftp://host").unwrap();
        let password = crate::credentials::Password::new("x".repeat(5000));
        let error = anyhow::anyhow!("failed: {}", password.expose());
        let message = connection_error(&target, &error, None, "", Some(&password));
        assert!(message.contains("[redacted]"));
        assert!(!message.contains("xxxx"));
        let long = anyhow::anyhow!("{}", "long failure ".repeat(1000));
        assert_eq!(
            connection_error(&target, &long, None, "", None)
                .chars()
                .count(),
            4096
        );
    }

    #[test]
    fn ssh_authentication_and_host_key_errors_are_not_retryable() {
        assert!(!transient_ssh_error("Permission denied (password)."));
        assert!(!transient_ssh_error("Host key verification failed."));
        assert!(transient_ssh_error(
            "ssh: connect to host server port 22: Connection refused"
        ));
    }
}
