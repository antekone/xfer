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
    fs::File,
    future::Future,
    io::Read,
    io::SeekFrom,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    process::{Child, Command},
    sync::{Mutex as AsyncMutex, MutexGuard},
    task::JoinHandle,
};

const IO_TIMEOUT: Duration = Duration::from_secs(30);
pub struct Sftp;

fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}
fn local_error(path: &Path, error: impl std::fmt::Display) -> anyhow::Error {
    permanent(format!("local file {}: {error}", path.display()))
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

    fn command(&self, program: &Path, timeout: u64, password: bool) -> Command {
        let mut command = Command::new(program);
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
        let recipe = ConnectRecipe {
            target,
            timeout,
            password: options.password.or_else(|| {
                options
                    .bookmark
                    .as_ref()
                    .and_then(|bookmark| bookmark.password.clone())
            }),
            program: PathBuf::from("ssh"),
        };
        let (session, home) = recipe.establish(true).await?;
        let home = home.expect("initial connection resolves home directory");
        Ok(Box::new(SftpConnection {
            recipe,
            session: AsyncMutex::new(Some(session)),
            cwd: home.clone(),
            home,
        }))
    }
}

/// Frozen connection settings, including credentials, retained only in memory
/// until this logical connection is closed or replaced. Reconnects never reload
/// a bookmark or accidentally adopt another session's password/directory.
struct ConnectRecipe {
    target: Target,
    timeout: u64,
    password: Option<crate::credentials::Password>,
    program: PathBuf,
}
impl ConnectRecipe {
    async fn establish(&self, initial: bool) -> Result<(SftpSession, Option<String>)> {
        let target = &self.target;
        let timeout = self.timeout;
        let askpass = self
            .password
            .clone()
            .map(Askpass::new)
            .transpose()
            .map_err(|error| {
                permanent(format!(
                    "cannot initialize private SSH password helper: {error:#}"
                ))
            })?;
        let mut command = target.command(&self.program, timeout, askpass.is_some());
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
            // Logical cwd/home are absolute paths kept by SftpConnection. A
            // recovery session must not reset them or re-resolve a changed ~.
            let home = if initial {
                stage = "resolve initial remote directory";
                let directory = target.initial_directory(sftp.support_expand_path());
                let home = sftp
                    .fs()
                    .canonicalize(&directory)
                    .await
                    .with_context(|| format!("{stage} {:?}", target.directory))?;
                stage = "stat initial remote directory";
                let metadata = sftp
                    .fs()
                    .metadata(&home)
                    .await
                    .with_context(|| format!("{stage} {}", home.display()))?;
                if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
                    bail!(
                        "SFTP initial path {} on {} is not a directory",
                        home.display(),
                        target.description()
                    );
                }
                Some(remote_path(&home)?)
            } else {
                None
            };
            Ok::<_, anyhow::Error>((sftp, home))
        })
        .await;
        match result {
            Ok(Ok((sftp, home))) => {
                drop(askpass); // No listening secret socket outside authentication.
                Ok((SftpSession { sftp, process }, home))
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
                let message =
                    connection_error(target, &error, status, &diagnostic, self.password.as_ref());
                let reconnect_transport = !initial
                    && error
                        .chain()
                        .filter_map(|cause| cause.downcast_ref::<openssh_sftp_client::Error>())
                        .any(transport_failure);
                if !permanent_ssh_error(&diagnostic)
                    && (timed_out || transient_ssh_error(&diagnostic) || reconnect_transport)
                {
                    return Err(transport(message));
                }
                Err(permanent(message))
            }
        }
    }
}

struct SftpSession {
    sftp: Client,
    process: SshProcess,
}

#[derive(Debug)]
struct TransportError(String);
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TransportError {}
fn transport(message: impl Into<String>) -> anyhow::Error {
    TransportError(message.into()).into()
}

fn transport_failure(error: &openssh_sftp_client::Error) -> bool {
    use openssh_sftp_client::Error;
    match error {
        Error::IOError(_)
        | Error::AwaitableError(_)
        | Error::BackgroundTaskFailure(_)
        | Error::TaskJoinError(_)
        | Error::SftpServerFailure(_) => true,
        Error::RecursiveErrors(errors) => {
            transport_failure(&errors.original_error) || transport_failure(&errors.occuring_error)
        }
        Error::RecursiveErrors3(errors) => {
            transport_failure(&errors.err1)
                || transport_failure(&errors.err2)
                || transport_failure(&errors.err3)
        }
        _ => false,
    }
}

fn permanent_ssh_error(diagnostic: &str) -> bool {
    let diagnostic = diagnostic.to_ascii_lowercase();
    [
        "permission denied",
        "authentication failed",
        "host key verification failed",
        "remote host identification has changed",
        "bad configuration",
        "bad owner or permissions",
        "no matching host key",
        "no matching cipher",
        "no supported authentication methods",
        "subsystem request failed",
        "unsupported ssh prompt",
    ]
    .iter()
    .any(|message| diagnostic.contains(message))
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
        Err(_) => return Err(transport("SFTP operation timed out; reconnecting on retry")),
    };
    if transport_failure(&error) {
        return Err(transport("SFTP transport failed; reconnecting on retry"));
    }
    let message = match error {
        Error::SftpError(SftpErrorKind::NoSuchFile, _) => "no such remote file or directory".into(),
        Error::SftpError(SftpErrorKind::PermDenied, _) => "remote permission denied".into(),
        Error::SftpError(kind, _) => format!("remote SFTP operation failed ({kind:?})"),
        _ => "invalid or unsupported SFTP response".into(),
    };
    Err(permanent(message))
}
fn record(
    name: String,
    metadata: &MetaData,
    directory: bool,
    owner: Option<String>,
    group: Option<String>,
) -> Content {
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
        owner,
        group,
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
/// Extract owner and group from the conventional SFTP v3 longname form.
/// The field is server-defined, so only accept an ls-like mode and link-count
/// prefix; numeric SFTP attributes remain the fallback when this fails.
fn owner_group_from_longname(longname: &[u8]) -> (Option<String>, Option<String>) {
    let Ok(longname) = std::str::from_utf8(longname) else {
        return (None, None);
    };
    let mut fields = longname.split_ascii_whitespace();
    let Some(mode) = fields.next() else {
        return (None, None);
    };
    let Some(links) = fields.next() else {
        return (None, None);
    };
    if mode.len() != 10
        || !matches!(
            mode.as_bytes()[0],
            b'-' | b'd' | b'l' | b'p' | b's' | b'b' | b'c'
        )
        || links.parse::<u64>().is_err()
    {
        return (None, None);
    }
    let (Some(owner), Some(group)) = (fields.next(), fields.next()) else {
        return (None, None);
    };
    (Some(owner.to_owned()), Some(group.to_owned()))
}

fn remote_file(metadata: &MetaData) -> Result<RemoteFile> {
    if !metadata.file_type().is_some_and(|kind| kind.is_file()) {
        return Err(permanent("expected a regular remote file"));
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
    recipe: ConnectRecipe,
    session: AsyncMutex<Option<SftpSession>>,
    cwd: String,
    home: String,
}

/// Take the session out of its slot while an operation runs. If the future is
/// cancelled, dropping this lease kills the in-flight process and leaves None,
/// so the next operation can recover instead of reusing a half-completed stream.
struct SessionLease<'a> {
    session: SftpSession,
    slot: MutexGuard<'a, Option<SftpSession>>,
}
impl SessionLease<'_> {
    fn finish<T>(self, result: Result<T>) -> Result<T> {
        let Self { session, mut slot } = self;
        if !result
            .as_ref()
            .is_err_and(|error| error.is::<TransportError>())
        {
            *slot = Some(session);
        }
        result
    }
}
impl SftpConnection {
    async fn acquire(&self) -> Result<SessionLease<'_>> {
        let mut slot = self.session.lock().await;
        let session = match slot.take() {
            Some(session) => session,
            None => self.recipe.establish(false).await?.0,
        };
        Ok(SessionLease { session, slot })
    }
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
        let lease = self.acquire().await?;
        let result = async {
            let canonical = operation(lease.session.sftp.fs().canonicalize(&path)).await?;
            let metadata = operation(lease.session.sftp.fs().metadata(&canonical)).await?;
            if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
                return Err(permanent("not a remote directory"));
            }
            remote_path(&canonical)
        }
        .await;
        self.cwd = lease.finish(result)?;
        Ok(())
    }
    async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        let path = normalize(&self.cwd, path);
        let lease = self.acquire().await?;
        let result = mkdir_on(&lease.session.sftp, &path, parents).await;
        lease.finish(result)
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let path = self.resolve_listing_path(path)?;
        let lease = self.acquire().await?;
        let result = list_on(&lease.session.sftp, &path, output).await;
        lease.finish(result)
    }
    async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
        let path = self.resolve_listing_path(path)?;
        let lease = self.acquire().await?;
        let result = async {
            let metadata = operation(lease.session.sftp.fs().metadata(path)).await?;
            remote_file(&metadata)
        }
        .await;
        lease.finish(result)
    }
    async fn upload(&self, local: &Path, path: &str, overwrite: bool) -> Result<()> {
        let path = self.resolve_listing_path(path)?;
        let lease = self.acquire().await?;
        let result = upload_on(&lease.session.sftp, local, &path, overwrite).await;
        lease.finish(result)
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
        let lease = self.acquire().await?;
        let result = download_on(&lease.session.sftp, &path, offset, expected, sink).await;
        lease.finish(result)
    }
    async fn disconnect(self: Box<Self>) -> Result<()> {
        let session = self.session.lock().await.take();
        drop(self); // Release retained credentials even if graceful close fails.
        let Some(SftpSession { sftp, mut process }) = session else {
            return Ok(());
        };
        let result = tokio::time::timeout(Duration::from_secs(2), sftp.close()).await;
        let _ = process.child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), process.child.wait()).await;
        match result {
            Ok(result) => result.map_err(|_| permanent("SFTP close failed")),
            Err(_) => Err(permanent("SFTP close timed out")),
        }
    }
}

async fn mkdir_on(sftp: &Client, path: &str, parents: bool) -> Result<()> {
    if !parents {
        return operation(sftp.fs().create_dir(path)).await;
    }
    let mut ancestor = String::new();
    for component in path.split('/').filter(|part| !part.is_empty()) {
        ancestor.push('/');
        ancestor.push_str(component);
        match operation(sftp.fs().create_dir(&ancestor)).await {
            Ok(()) => {}
            Err(error) if error.is::<TransportError>() => return Err(error),
            Err(error) => {
                // SFTP v3 often reports only Failure for an existing entry.
                // Follow symlinks and accept only a verified directory;
                // unreadable/missing paths and file collisions remain errors.
                match operation(sftp.fs().metadata(&ancestor)).await {
                    Ok(metadata) if metadata.file_type().is_some_and(|kind| kind.is_dir()) => {}
                    Ok(_) => {
                        return Err(permanent(format!("not a remote directory: {ancestor:?}")))
                    }
                    Err(error) if error.is::<TransportError>() => return Err(error),
                    Err(_) => return Err(error),
                }
            }
        }
    }
    Ok(())
}

async fn list_on(sftp: &Client, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
    let metadata = operation(sftp.fs().metadata(&path)).await?;
    if !metadata.file_type().is_some_and(|kind| kind.is_dir()) {
        if directory_input(path) {
            return Err(permanent("not a remote directory"));
        }
        let name = path.rsplit('/').next().unwrap_or(path).to_owned();
        output.emit_file(record(name, &metadata, false, None, None))?;
        return Ok(1);
    }
    let dir = operation(sftp.fs().open_dir(&path)).await?;
    let mut entries = Box::pin(dir.read_dir());
    let mut count = 0;
    loop {
        let entry = tokio::time::timeout(IO_TIMEOUT, entries.next())
            .await
            .map_err(|_| transport("SFTP directory read timed out; reconnecting on retry"))?;
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
            match operation(
                sftp.fs()
                    .metadata(format!("{}/{name}", path.trim_end_matches('/'))),
            )
            .await
            {
                Ok(followed) => directory = followed.file_type().is_some_and(|kind| kind.is_dir()),
                Err(error) if error.is::<TransportError>() => return Err(error),
                Err(_) => {} // A broken/unreadable symlink is still a valid listing entry.
            }
        }
        let (owner, group) = owner_group_from_longname(entry.longname());
        output.emit(record(name.to_owned(), &metadata, directory, owner, group))?;
        count += 1;
    }
    Ok(count)
}

async fn upload_on(sftp: &Client, local: &Path, remote: &str, overwrite: bool) -> Result<()> {
    let mut input = File::open(local).map_err(|error| local_error(local, error))?;
    let mut options = sftp.options();
    options.write(true);
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut output = operation(options.open(remote)).await?;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| local_error(local, error))?;
        if read == 0 {
            break;
        }
        operation(output.write_all(&buffer[..read])).await?;
    }
    operation(output.close()).await
}

async fn download_on(
    sftp: &Client,
    path: &str,
    offset: u64,
    expected: &RemoteFile,
    sink: &mut dyn DownloadSink,
) -> Result<()> {
    if offset > expected.size {
        return Err(permanent("resume offset exceeds remote size"));
    }
    let mut file = operation(sftp.open(path)).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use openssh_sftp_client::metadata::{MetaDataBuilder, Permissions, RawPermissions};

    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    async fn reconnect_fixture() -> Option<(tempfile::TempDir, SftpConnection)> {
        use std::os::unix::fs::PermissionsExt;
        let server = [
            "/usr/lib/ssh/sftp-server",
            "/usr/lib/openssh/sftp-server",
            "/usr/libexec/sftp-server",
        ]
        .into_iter()
        .find(|path| Path::new(path).exists());
        let Some(server) = server else {
            eprintln!("sftp-server unavailable; skipping protocol test");
            return None;
        };
        let root = tempfile::tempdir().unwrap();
        let remote = root.path().join("remote");
        std::fs::create_dir_all(remote.join("photos")).unwrap();
        std::fs::write(
            remote.join("photos/file.bin"),
            (0..200_000).map(|n| (n % 251) as u8).collect::<Vec<_>>(),
        )
        .unwrap();
        let quote = |path: &Path| {
            shlex::try_quote(path.to_str().unwrap())
                .unwrap()
                .into_owned()
        };
        let program = root.path().join("ssh-fixture");
        std::fs::write(&program, format!(
            "#!/bin/sh\necho $$ >> {}\nif [ -f {} ]; then echo 'Permission denied (password).' >&2; exit 255; fi\nif [ -f {} ]; then exec sleep 30; fi\nexec {} -d {}\n",
            quote(&root.path().join("pids")), quote(&root.path().join("deny")), quote(&root.path().join("hang")), server, quote(&remote),
        )).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let recipe = ConnectRecipe {
            target: Target {
                host: "fixture".into(),
                user: None,
                port: None,
                directory: remote.to_str().unwrap().into(),
            },
            timeout: 5,
            password: Some(crate::credentials::Password::new("fixture secret".into())),
            program,
        };
        let (session, home) = recipe.establish(true).await.unwrap();
        let home = home.unwrap();
        let mut connection = SftpConnection {
            recipe,
            session: AsyncMutex::new(Some(session)),
            cwd: home.clone(),
            home,
        };
        connection.change_directory("photos").await.unwrap();
        Some((root, connection))
    }

    struct InterruptOnce<'a> {
        connection: &'a SftpConnection,
        pid: u32,
        interrupted: AtomicBool,
        offsets: Mutex<Vec<u64>>,
        after_interrupt: Option<Box<dyn Fn() + Send + Sync>>,
    }
    struct InterruptSink<'a> {
        inner: &'a mut dyn DownloadSink,
        connection: &'a InterruptOnce<'a>,
    }
    impl DownloadSink for InterruptSink<'_> {
        fn write(&mut self, bytes: &[u8]) -> Result<()> {
            self.inner.write(bytes)?;
            if !self.connection.interrupted.swap(true, Ordering::SeqCst) {
                // SAFETY: signal only the child SFTP process created by this test.
                assert_eq!(
                    unsafe { libc::kill(self.connection.pid as i32, libc::SIGKILL) },
                    0
                );
                if let Some(action) = &self.connection.after_interrupt {
                    action();
                }
            }
            Ok(())
        }
    }
    #[async_trait]
    impl Connection for InterruptOnce<'_> {
        fn protocol(&self) -> &'static str {
            "sftp"
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            self.connection.resolve_listing_path(path)
        }
        async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
            self.connection.stat_file(path).await
        }
        async fn download(
            &self,
            path: &str,
            offset: u64,
            expected: &RemoteFile,
            sink: &mut dyn DownloadSink,
        ) -> Result<()> {
            self.offsets.lock().unwrap().push(offset);
            self.connection
                .download(
                    path,
                    offset,
                    expected,
                    &mut InterruptSink {
                        inner: sink,
                        connection: self,
                    },
                )
                .await
        }
    }

    async fn interrupted_connection(connection: &SftpConnection) -> InterruptOnce<'_> {
        let pid = connection
            .session
            .lock()
            .await
            .as_ref()
            .unwrap()
            .process
            .child
            .id()
            .unwrap();
        InterruptOnce {
            connection,
            pid,
            interrupted: AtomicBool::new(false),
            offsets: Mutex::new(Vec::new()),
            after_interrupt: None,
        }
    }
    async fn get_fixture(connection: &dyn Connection, root: &Path) -> Result<()> {
        let directory = root.join("downloads");
        std::fs::create_dir_all(&directory).unwrap();
        let (progress, _) = tokio::sync::watch::channel(crate::core::Progress::Indeterminate {
            message: String::new(),
        });
        static NEXT_OUTPUT: AtomicU64 = AtomicU64::new(987654);
        let output = crate::core::OutputStore::new(NEXT_OUTPUT.fetch_add(1, Ordering::Relaxed));
        let output_path = output.path().to_owned();
        let result = crate::transfer::get(
            connection,
            crate::transfer::GetArgs {
                resume: false,
                files: vec!["file.bin".into()],
            },
            directory,
            false,
            progress,
            output,
        )
        .await;
        let _ = std::fs::remove_file(output_path);
        result.map(|_| ())
    }

    #[tokio::test]
    async fn get_reconnects_after_a_partial_read_and_resumes_without_duplicate_bytes() {
        let Some((root, connection)) = reconnect_fixture().await else {
            return;
        };
        let cwd = connection.pwd().unwrap();
        let home = connection.resolve_directory(None).unwrap();
        let interrupted = interrupted_connection(&connection).await;
        get_fixture(&interrupted, root.path()).await.unwrap();
        let offsets = interrupted.offsets.lock().unwrap().clone();
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0], 0);
        assert!(offsets[1] > 0 && offsets[1] < 200_000, "{offsets:?}");
        assert_eq!(
            std::fs::read(root.path().join("downloads/file.bin")).unwrap(),
            std::fs::read(root.path().join("remote/photos/file.bin")).unwrap()
        );
        assert_eq!(connection.pwd().unwrap(), cwd);
        assert_eq!(connection.resolve_directory(None).unwrap(), home);
        assert_eq!(
            connection.recipe.password.as_ref().unwrap().expose(),
            "fixture secret"
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("pids"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        Box::new(connection).disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn changed_remote_file_after_reconnect_is_not_appended() {
        let Some((root, connection)) = reconnect_fixture().await else {
            return;
        };
        let mut interrupted = interrupted_connection(&connection).await;
        let path = root.path().join("remote/photos/file.bin");
        interrupted.after_interrupt = Some(Box::new(move || {
            std::fs::write(&path, b"changed").unwrap();
        }));
        let error = get_fixture(&interrupted, root.path()).await.unwrap_err();
        assert!(error.is::<NonRetryableError>(), "{error}");
        assert!(error.to_string().contains("remote file changed"));
        let partial = std::fs::read(root.path().join("downloads/file.bin")).unwrap();
        assert!(!partial.is_empty() && partial.len() < 200_000);
        assert_eq!(
            partial,
            (0..partial.len())
                .map(|n| (n % 251) as u8)
                .collect::<Vec<_>>()
        );
        Box::new(connection).disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn authentication_failure_on_reconnect_stops_retrying() {
        let Some((root, connection)) = reconnect_fixture().await else {
            return;
        };
        let mut interrupted = interrupted_connection(&connection).await;
        let deny = root.path().join("deny");
        interrupted.after_interrupt = Some(Box::new(move || {
            std::fs::write(&deny, b"").unwrap();
        }));
        let error = get_fixture(&interrupted, root.path()).await.unwrap_err();
        assert!(error.is::<NonRetryableError>(), "{error}");
        assert!(error.to_string().contains("Permission denied"));
        assert!(!error.to_string().contains("fixture secret"));
        assert_eq!(
            std::fs::read_to_string(root.path().join("pids"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        Box::new(connection).disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_session_or_reconnect_can_be_recovered_by_the_next_operation() {
        let Some((root, connection)) = reconnect_fixture().await else {
            return;
        };
        // A cancelled operation drops its lease instead of returning the live
        // protocol stream to the slot with outstanding requests.
        let lease = connection.acquire().await.unwrap();
        drop(lease);
        assert!(connection.session.lock().await.is_none());
        let hang = root.path().join("hang");
        std::fs::write(&hang, b"").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection.stat_file("file.bin"))
                .await
                .is_err()
        );
        assert!(connection.session.lock().await.is_none());
        std::fs::remove_file(&hang).unwrap();
        assert_eq!(
            connection.stat_file("file.bin").await.unwrap().size,
            200_000
        );
        assert!(connection.recipe.password.is_some());
        Box::new(connection).disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn missing_files_keep_the_live_session_and_do_not_reconnect() {
        let Some((root, connection)) = reconnect_fixture().await else {
            return;
        };
        assert!(connection
            .stat_file("missing")
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        assert_eq!(
            connection.stat_file("file.bin").await.unwrap().size,
            200_000
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("pids"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        Box::new(connection).disconnect().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn transport_errors_and_operation_timeouts_are_retryable() {
        let error = operation::<()>(async {
            Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
        })
        .await
        .unwrap_err();
        assert!(error.is::<TransportError>());
        assert!(!error.is::<NonRetryableError>());
        let error = operation::<()>(std::future::pending()).await.unwrap_err();
        assert!(error.is::<TransportError>());
        let error = operation::<()>(async {
            Err(openssh_sftp_client::Error::UnsupportedSftpProtocol { version: 4 })
        })
        .await
        .unwrap_err();
        assert!(error.is::<NonRetryableError>());
    }

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
    fn longname_owner_group_is_best_effort() {
        assert_eq!(
            owner_group_from_longname(b"-rw-r--r-- 1 alice developers 83 Dec 01 17:12 ip6.txt"),
            (Some("alice".into()), Some("developers".into()))
        );
        assert_eq!(owner_group_from_longname(b"not an ls line"), (None, None));
        assert_eq!(owner_group_from_longname(b"\xff"), (None, None));
    }

    #[test]
    fn ssh_command_is_subsystem_only_and_verifies_host_keys() {
        let target = Target::parse("sftp://user@example.org:2222").unwrap();
        let command = target.command(Path::new("ssh"), 5, false);
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
            .command(Path::new("ssh"), 5, true)
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
        let Content::File(file) = record("file.txt".into(), &metadata, false, None, None) else {
            panic!("expected file");
        };
        assert_eq!(file.size, Some(200000));
        assert_eq!(file.modified.unwrap().timestamp(), 1234567890);
        assert_eq!(
            file.unix,
            Some(UnixMetadata {
                mode: Some(0o100640),
                uid: Some(1234),
                gid: Some(5678),
                owner: None,
                group: None,
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
            let Content::Directory(directory) =
                record("photos".into(), &metadata, true, None, None)
            else {
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
        assert!(permanent_ssh_error("Permission denied (password)."));
        assert!(permanent_ssh_error("Host key verification failed."));
        assert!(permanent_ssh_error(
            "REMOTE HOST IDENTIFICATION HAS CHANGED!"
        ));
        assert!(!permanent_ssh_error("Connection reset by peer"));
        assert!(!transient_ssh_error("Permission denied (password)."));
        assert!(!transient_ssh_error("Host key verification failed."));
        assert!(transient_ssh_error(
            "ssh: connect to host server port 22: Connection refused"
        ));
    }
}
