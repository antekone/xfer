//! Daemon-owned application state and local per-session IPC server.

// ------------------------------------------------------------------------------------------------

use std::{
    collections::HashMap,
    env, fs,
    io::Write,
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

use crate::{
    backend::{Backend, Sftp, Smb, Webdav, S3},
    core::{JobManager, JobState, SettingValue, Settings},
};
use anyhow::{Context, Result};
use zeroize::Zeroizing;

// ------------------------------------------------------------------------------------------------

fn default_history_path() -> String {
    crate::state_dir().join("history").display().to_string()
}

/// Lightweight committed session state: safe to query while a job owns the
/// backend connection lock. It never contains credentials or endpoint secrets.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SessionInfo {
    pub daemon_pid: u32,
    pub protocol: Option<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub prompt: Option<String>,
}
impl From<crate::ipc::pb::Session> for SessionInfo {
    fn from(value: crate::ipc::pb::Session) -> Self {
        Self {
            daemon_pid: value.daemon_pid,
            protocol: value.protocol,
            cwd: value.cwd,
            user: value.user,
            host: value.host,
            prompt: value.prompt,
        }
    }
}
impl From<SessionInfo> for crate::ipc::pb::Session {
    fn from(value: SessionInfo) -> Self {
        Self {
            daemon_pid: value.daemon_pid,
            protocol: value.protocol,
            cwd: value.cwd,
            user: value.user,
            host: value.host,
            prompt: value.prompt,
        }
    }
}
impl SessionInfo {
    pub(crate) fn disconnected() -> Self {
        Self {
            daemon_pid: std::process::id(),
            protocol: None,
            cwd: None,
            ..Self::default()
        }
    }
    pub(crate) fn connected(connection: &dyn crate::backend::Connection) -> Self {
        Self {
            daemon_pid: std::process::id(),
            protocol: Some(connection.protocol().to_owned()),
            cwd: connection.pwd().ok(),
            user: connection.prompt_user(),
            host: connection.prompt_host(),
            prompt: None,
        }
    }
}

pub(crate) struct ConfigFile {
    path: PathBuf,
    loaded: bool,
}

impl ConfigFile {
    fn load(settings: &mut Settings, path: PathBuf) -> Result<Self> {
        let path = std::path::absolute(path)?;
        let loaded = settings.load_file(&path)?;
        Ok(Self { path, loaded })
    }

    pub(crate) fn status(&self, label: &str) -> String {
        format!(
            "{label}: {}{}",
            self.path.display(),
            if self.loaded {
                ""
            } else {
                " (not loaded: file not found)"
            }
        )
    }
}

/// Authoritative daemon state. Clients interact with it through IPC and do not
/// own jobs, connections, or backend configuration.
pub struct Daemon {
    pub settings: Settings,
    pub jobs: JobManager,
    pub(crate) backends: HashMap<String, std::sync::Arc<dyn Backend>>,
    pub(crate) connection:
        std::sync::Arc<tokio::sync::Mutex<Option<Box<dyn crate::backend::Connection>>>>,
    pub(crate) session: std::sync::Arc<std::sync::Mutex<SessionInfo>>,
    pub(crate) local_directory: PathBuf,
    pub(crate) config_file: ConfigFile,
    pub(crate) config_override: Option<ConfigFile>,
    pub(crate) startup_file: Option<PathBuf>,
}

impl Daemon {
    pub fn bootstrap() -> Result<Self> {
        let mut settings = Settings::new();
        settings.register(
            "ssl:skip-validation",
            SettingValue::Bool(false),
            "Disable TLS certificate validation",
        );
        settings.register(
            "ui:history-file",
            SettingValue::Text(default_history_path()),
            "Client history path; session or /dev/null disables persistence",
        );
        settings.register(
            "xfer:clobber",
            SettingValue::Bool(false),
            "Allow get to overwrite local files and put to overwrite remote files",
        );
        crate::commands::ls::register_settings(&mut settings);
        crate::cache::CacheOptions::register(&mut settings);
        let backends: Vec<Box<dyn Backend>> = vec![
            Box::new(Sftp),
            Box::new(S3),
            Box::new(Smb),
            Box::new(Webdav),
        ];
        for backend in &backends {
            backend.register_settings(&mut settings);
        }
        settings.register(
            "bookmarks:file",
            SettingValue::Text(crate::bookmarks::default_path().display().to_string()),
            "TOML bookmarks file",
        );
        settings.register(
            "ui:auto-pager",
            SettingValue::Bool(true),
            "Use pager for long output",
        );
        settings.register(
            "ui:prompt",
            SettingValue::Text("xfer$location> ".into()),
            "Prompt template: $cwd, $cwdn, $user, $host, $proto, $lcwd, $lcwdn, $location; $$ escapes $",
        );
        let config_file = ConfigFile::load(&mut settings, crate::config_dir().join("xfer.conf"))?;
        let config_override = std::env::var_os("XFER_CONFIG")
            .map(|path| ConfigFile::load(&mut settings, PathBuf::from(path)))
            .transpose()?;
        for backend in &backends {
            backend.initialize(&settings)?;
        }
        Ok(Self {
            settings,
            jobs: JobManager::new(),
            backends: backends
                .into_iter()
                .map(|backend| {
                    (
                        backend.protocol().to_owned(),
                        std::sync::Arc::<dyn Backend>::from(backend as Box<dyn Backend>),
                    )
                })
                .collect(),
            connection: Default::default(),
            session: std::sync::Arc::new(std::sync::Mutex::new(SessionInfo::disconnected())),
            local_directory: std::env::current_dir()?,
            config_file,
            config_override,
            startup_file: None,
        })
    }

    /// Bootstrap command dispatch. Protocol backends will replace demo jobs.
    pub fn has_running_jobs(&mut self) -> bool {
        self.jobs.poll();
        self.jobs
            .jobs
            .values()
            .any(|job| job.state == JobState::Running)
    }

    pub fn connect_command(
        &mut self,
        target: &str,
        password: Option<crate::credentials::Password>,
    ) -> Vec<String> {
        crate::commands::dispatch_connect(self, target, password)
    }

    pub fn set_local_directory(&mut self, path: PathBuf) -> Result<()> {
        if !path.is_absolute() {
            anyhow::bail!("local directory must be absolute");
        }
        let path = path.canonicalize()?;
        if !path.is_dir() {
            anyhow::bail!("local directory is not a directory");
        }
        self.local_directory = path;
        Ok(())
    }

    /// Remember the client-side rc file before dispatching its startup commands.
    pub fn set_startup_file(&mut self, path: PathBuf) {
        self.startup_file = Some(path);
    }

    pub fn session_info(&self) -> SessionInfo {
        let mut info = self.session.lock().expect("session state poisoned").clone();
        info.prompt = Some(crate::prompt::format(
            self.settings.text("ui:prompt").unwrap_or("xfer$location> "),
            &info,
            &self.local_directory.to_string_lossy(),
        ));
        info
    }

    pub fn bookmark_completion(&self, input: &str) -> Result<crate::cache::PathCompletion> {
        let bookmarks = crate::bookmarks::load(std::path::Path::new(
            self.settings.text("bookmarks:file").unwrap(),
        ))?;
        Ok(crate::cache::PathCompletion {
            candidates: bookmarks
                .into_keys()
                .filter(|name| name.starts_with(input))
                .collect(),
            truncated: false,
        })
    }

    pub fn path_completion(
        &self,
        input: String,
        directories_only: bool,
    ) -> impl std::future::Future<Output = Result<crate::cache::PathCompletion>> + Send + 'static
    {
        let connection = self.connection.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(3), async move {
                let active = connection.lock().await;
                let active = active
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("not connected; use connect first"))?;
                if directories_only {
                    active.complete_directories(&input).await
                } else {
                    active.complete_paths(&input).await
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("path completion timed out; try ls first"))?
        }
    }

    pub fn command(&mut self, line: &str) -> Vec<String> {
        self.jobs.poll();
        crate::commands::dispatch_daemon(self, line)
    }
}

/// State shared between the accept loop and per-connection worker threads.
struct Shared {
    attached: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    startup_claimed: Arc<AtomicBool>,
    daemon: Arc<Mutex<Daemon>>,
}

/// Test harnesses spawn the daemon directly; their parent is the test binary,
/// not the xfer client, so they must opt out of the parent check explicitly.
pub const ALLOW_FOREIGN_PARENT_ENV: &str = "XFER_DAEMON_ALLOW_FOREIGN_PARENT";

/// The daemon is an internal mode that the client spawns from its own
/// executable. Refuse direct shell execution by requiring the parent process to
/// be this same binary. This is a usability guard, not a security boundary:
/// the environment override exists for test harnesses.
fn verify_spawned_by_client() -> Result<()> {
    if env::var_os(ALLOW_FOREIGN_PARENT_ENV).is_some() {
        return Ok(());
    }

    let own = env::current_exe()?
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_exe().expect("current_exe exists"));

    let parent = unsafe { libc::getppid() };
    let parent = if parent <= 1 {
        None
    } else {
        fs::read_link(format!("/proc/{parent}/exe"))
            .ok()
            .and_then(|path| path.canonicalize().ok())
    };

    if parent.as_deref() != Some(own.as_path()) {
        anyhow::bail!(
            "`xfer daemon` is internal and must be spawned by the xfer client; \
             start `xfer` normally or attach with `xfer attach`"
        );
    }

    Ok(())
}

pub async fn main() -> Result<()> {
    verify_spawned_by_client()?;

    let daemon = Daemon::bootstrap()?;
    let mut socket = DaemonSocket::bind_in(&crate::runtime_dir()?)?;
    let shared = Shared {
        attached: Arc::new(AtomicBool::new(false)),
        shutdown: Arc::new(AtomicBool::new(false)),
        startup_claimed: Arc::new(AtomicBool::new(false)),
        daemon: Arc::new(Mutex::new(daemon)),
    };

    socket.publish()?;
    // stdout is a private, one-message startup channel to the spawning client.
    // Announce only after initialization, binding, and atomic publication succeed.
    socket.announce(&mut std::io::stdout().lock())?;

    while !shared.shutdown.load(Ordering::Acquire) {
        let stream = match socket.listener.accept() {
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
    Ok(())
}

/// Each daemon owns a freshly allocated private directory, never a PID-only
/// socket pathname that could belong to another process or PID namespace.
/// RAII unpublishes discovery metadata before removing the socket directory,
/// including on initialization/publication/announcement errors.
struct DaemonSocket {
    listener: UnixListener,
    directory: tempfile::TempDir,
    metadata: Option<PathBuf>,
    id: String,
}

impl DaemonSocket {
    fn bind_in(runtime: &std::path::Path) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix(&format!("{}-", std::process::id()))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(runtime)?;
        let id = directory
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .context("invalid daemon directory name")?
            .to_owned();
        let path = directory.path().join("s");
        // bind itself is atomic. Never unlink an existing socket to make room.
        let listener =
            UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            directory,
            metadata: None,
            id,
        })
    }

    fn description(&self) -> Result<serde_json::Value> {
        let path = self.directory.path().join("s");
        let path = path
            .to_str()
            .context("daemon socket path must be valid UTF-8")?;
        Ok(serde_json::json!({
            "id": self.id,
            "pid": std::process::id(),
            "socket": path,
        }))
    }

    fn publish(&mut self) -> Result<()> {
        let runtime = self.directory.path().parent().unwrap();
        let metadata = runtime.join(format!("{}.meta", self.id));
        // NamedTempFile starts at 0600; readers see either no metadata or the
        // whole record. No existing discovery entry may be overwritten.
        let mut temporary = tempfile::NamedTempFile::new_in(runtime)?;
        self.announce(&mut temporary)?;
        temporary
            .persist_noclobber(&metadata)
            .map_err(|error| error.error)?;
        self.metadata = Some(metadata);
        Ok(())
    }

    fn announce(&self, writer: &mut impl Write) -> Result<()> {
        serde_json::to_writer(&mut *writer, &self.description()?)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }
}

impl Drop for DaemonSocket {
    fn drop(&mut self) {
        if let Some(metadata) = &self.metadata {
            let _ = fs::remove_file(metadata);
        }
        // TempDir removes the socket next; each pathname is unique to us.
    }
}

/// One worker per connection. Every wire message is a bounded protobuf frame;
/// protocol errors never reach command parsing or acquire the attachment lease.
fn serve_connection(shared: Shared, mut stream: UnixStream, runtime: tokio::runtime::Handle) {
    use crate::ipc::{self, pb, Op, Reply};
    let _runtime_guard = runtime.enter();
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut lease = None;
    let mut last_id = 0;
    while let Ok(Some(request)) = ipc::read_frame::<pb::Request>(&mut stream) {
        let id = request.id;
        let version_ok = request.version == ipc::VERSION;
        let result = if !version_ok {
            Err(rpc_error(
                pb::ErrorCode::UnsupportedVersion,
                "incompatible daemon IPC version; restart the daemon",
            ))
        } else if id == 0 || id <= last_id {
            Err(rpc_error(
                pb::ErrorCode::InvalidRequest,
                "invalid IPC request ID",
            ))
        } else if let Some(operation) = request.operation {
            last_id = id;
            match operation {
                Op::Probe(_) if lease.is_none() => {
                    let result = Reply::Probe(pb::ProbeResult {
                        available: !shared.shutdown.load(Ordering::Acquire)
                            && !shared.attached.load(Ordering::Acquire),
                    });
                    let _ = send_response(&mut stream, id, Ok(result));
                    return;
                }
                Op::Attach(_) if lease.is_none() => {
                    // Serialize acquisition with lease release/shutdown decisions.
                    let _state = shared.daemon.lock().expect("daemon state poisoned");
                    if shared.shutdown.load(Ordering::Acquire) {
                        Err(rpc_error(
                            pb::ErrorCode::NotAttached,
                            "daemon is shutting down",
                        ))
                    } else if shared
                        .attached
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        Err(rpc_error(pb::ErrorCode::Busy, "daemon is already attached"))
                    } else {
                        lease = Some(AttachmentLease(&shared));
                        let _ = stream.set_read_timeout(None);
                        Ok(Reply::Attached(pb::Empty {}))
                    }
                }
                _ if lease.is_none() => Err(rpc_error(
                    pb::ErrorCode::NotAttached,
                    "attach before issuing requests",
                )),
                Op::Attach(_) | Op::Probe(_) => {
                    Err(rpc_error(pb::ErrorCode::InvalidRequest, "already attached"))
                }
                Op::Detach(_) => {
                    let _ = send_response(&mut stream, id, Ok(Reply::Ok(pb::Empty {})));
                    break;
                }
                operation => handle_request(&shared, operation, &runtime),
            }
        } else {
            last_id = id;
            Err(rpc_error(
                pb::ErrorCode::InvalidRequest,
                "missing or unsupported IPC operation",
            ))
        };
        if send_response(&mut stream, id, result).is_err() || !version_ok {
            break;
        }
    }
    // AttachmentLease releases ownership even on broken pipes and unwinding.
}

struct AttachmentLease<'a>(&'a Shared);
impl Drop for AttachmentLease<'_> {
    fn drop(&mut self) {
        if let Ok(mut daemon) = self.0.daemon.lock() {
            // Modal snapshots belong to this attachment, not background work.
            // Abrupt EOF/unwind must not leave scans or private files orphaned.
            let directory_jobs = daemon
                .jobs
                .jobs
                .iter()
                .filter_map(|(&id, job)| job.directory.is_some().then_some(id))
                .collect::<Vec<_>>();
            for id in directory_jobs {
                daemon.jobs.cancel(id);
                if let Some(job) = daemon.jobs.jobs.remove(&id) {
                    let _ = fs::remove_file(job.output.path());
                }
            }
            self.0.attached.store(false, Ordering::Release);
            if !daemon.has_running_jobs() {
                self.0.shutdown.store(true, Ordering::Release);
            }
        } else {
            self.0.attached.store(false, Ordering::Release);
            self.0.shutdown.store(true, Ordering::Release);
        }
    }
}

type RpcResult = std::result::Result<crate::ipc::Reply, crate::ipc::pb::Error>;
fn rpc_error(code: crate::ipc::pb::ErrorCode, message: impl Into<String>) -> crate::ipc::pb::Error {
    crate::ipc::pb::Error {
        code: code as i32,
        message: message.into(),
    }
}
fn operation_error(error: impl std::fmt::Display) -> crate::ipc::pb::Error {
    rpc_error(
        crate::ipc::pb::ErrorCode::OperationFailed,
        error.to_string(),
    )
}
fn send_response(stream: &mut UnixStream, id: u64, result: RpcResult) -> std::io::Result<()> {
    use crate::ipc::{self, pb, Reply};
    use prost::Message;
    let mut response = pb::Response {
        version: ipc::VERSION,
        id,
        result: Some(result.unwrap_or_else(Reply::Error)),
    };
    if response.encoded_len() > ipc::MAX_FRAME {
        response.result = Some(Reply::Error(rpc_error(
            pb::ErrorCode::ResponseTooLarge,
            "IPC response is too large; narrow the query or use a smaller page",
        )));
    }
    ipc::write_frame(stream, &response)
}

fn command_reply(result: Result<crate::commands::CommandResult>) -> RpcResult {
    use crate::{
        commands::CommandResult as C,
        ipc::{pb, Reply},
    };
    let value = result.map_err(operation_error)?;
    let mut reply = pb::CommandResult::default();
    reply.action = match value {
        C::Output(lines) => {
            reply.lines = lines;
            None
        }
        C::JobStarted(id) => Some(pb::command_result::Action::JobStarted(pb::JobId { id })),
        C::PasswordRequired(target) => Some(pb::command_result::Action::PasswordRequired(
            pb::PasswordPrompt { target },
        )),
        C::EditBookmarks(path) => Some(pb::command_result::Action::EditBookmarks(pb::Path {
            path: path.to_string_lossy().into_owned(),
        })),
        C::Detach => {
            return Err(rpc_error(
                pb::ErrorCode::InvalidRequest,
                "detach is a client operation",
            ))
        }
    };
    Ok(Reply::Command(reply))
}

fn handle_request(
    shared: &Shared,
    operation: crate::ipc::Op,
    runtime: &tokio::runtime::Handle,
) -> RpcResult {
    use crate::ipc::{self, pb, Op, Reply};
    if let Op::Complete(request) = operation {
        return handle_completion(shared, request, runtime);
    }
    let mut daemon = shared.daemon.lock().expect("daemon state poisoned");
    daemon.jobs.poll();
    let ok = || Ok(Reply::Ok(pb::Empty {}));
    let missing = || rpc_error(pb::ErrorCode::NotFound, "no such job");
    match operation {
        Op::StartDirectory(request) => {
            let source = pb::DirectorySource::try_from(request.source)
                .ok()
                .filter(|v| *v != pb::DirectorySource::Unspecified)
                .ok_or_else(|| {
                    rpc_error(pb::ErrorCode::InvalidRequest, "unknown directory source")
                })?;
            let filter = pb::DirectoryFilter::try_from(request.filter).map_err(|_| {
                rpc_error(pb::ErrorCode::InvalidRequest, "unknown directory filter")
            })?;
            let local = daemon.local_directory.clone();
            let connection = daemon.connection.clone();
            let slot = Arc::new(Mutex::new(None));
            let result_slot = slot.clone();
            let id = daemon.jobs.start_task(
                "file selector directory listing".into(),
                move |_, _| async move {
                    let snapshot = if source == pb::DirectorySource::Local {
                        crate::directory::Snapshot::local(&local, filter).await?
                    } else {
                        let active = connection.lock().await;
                        let active = active
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("not connected; use connect first"))?;
                        let path = active.resolve_directory(Some("."))?;
                        let mut snapshot = crate::directory::Snapshot::new(path.clone(), filter)?;
                        active.list(&path, &mut snapshot).await?;
                        snapshot.finish()?;
                        snapshot
                    };
                    let snapshot = snapshot.alphabetical()?.await?;
                    *result_slot.lock().expect("directory result poisoned") = Some(snapshot);
                    Ok(String::new())
                },
            );
            daemon.jobs.jobs.get_mut(&id).unwrap().directory = Some(slot);
            Ok(Reply::DirectoryStarted(pb::JobId { id }))
        }
        Op::FilterDirectory(request) => {
            let order = pb::DirectorySort::try_from(request.sort).map_err(|_| {
                rpc_error(
                    pb::ErrorCode::InvalidRequest,
                    "unknown directory sort order",
                )
            })?;
            if request.regexp.len() > 4096 {
                return Err(rpc_error(
                    pb::ErrorCode::InvalidRequest,
                    "file filter exceeds 4096 bytes",
                ));
            }
            let regexp = regex::RegexBuilder::new(&request.regexp)
                .size_limit(1024 * 1024)
                .dfa_size_limit(1024 * 1024)
                .build()
                .map_err(|error| {
                    rpc_error(
                        pb::ErrorCode::InvalidRequest,
                        format!(
                            "invalid regexp: {}",
                            error
                                .to_string()
                                .lines()
                                .last()
                                .unwrap_or("invalid expression")
                        ),
                    )
                })?;
            let job = daemon.jobs.jobs.get(&request.job_id).ok_or_else(missing)?;
            if job.state != JobState::Completed {
                return Err(operation_error("directory snapshot is not complete"));
            }
            let input = job
                .directory
                .as_ref()
                .ok_or_else(|| {
                    rpc_error(
                        pb::ErrorCode::InvalidRequest,
                        "not a directory snapshot job",
                    )
                })?
                .lock()
                .expect("directory result poisoned");
            let future = input
                .as_ref()
                .ok_or_else(|| operation_error("directory snapshot is unavailable"))?
                .filtered(regexp, order)
                .map_err(operation_error)?;
            drop(input);
            let slot = Arc::new(Mutex::new(None));
            let result_slot = slot.clone();
            let id = daemon.jobs.start_task(
                "file selector regexp filter".into(),
                move |_, _| async move {
                    let snapshot = future.await?;
                    *result_slot.lock().expect("directory result poisoned") = Some(snapshot);
                    Ok(String::new())
                },
            );
            daemon.jobs.jobs.get_mut(&id).unwrap().directory = Some(slot);
            Ok(Reply::DirectoryStarted(pb::JobId { id }))
        }
        Op::ReadDirectory(request) => {
            let job = daemon.jobs.jobs.get(&request.job_id).ok_or_else(missing)?;
            let slot = job.directory.as_ref().ok_or_else(|| {
                rpc_error(
                    pb::ErrorCode::InvalidRequest,
                    "not a directory snapshot job",
                )
            })?;
            if job.state != JobState::Completed {
                return Err(operation_error("directory snapshot is not complete"));
            }
            let snapshot = slot.lock().expect("directory result poisoned");
            let snapshot = snapshot
                .as_ref()
                .ok_or_else(|| operation_error("directory snapshot is unavailable"))?;
            Ok(Reply::DirectoryPage(
                snapshot
                    .page(request.offset, request.limit)
                    .map_err(operation_error)?,
            ))
        }
        Op::CloseDirectory(job) => {
            let value = daemon.jobs.jobs.get(&job.id).ok_or_else(missing)?;
            if value.directory.is_none() {
                return Err(rpc_error(
                    pb::ErrorCode::InvalidRequest,
                    "not a directory snapshot job",
                ));
            }
            let output = value.output.path().to_owned();
            // The task owns a cancellation token and its private partial snapshot.
            // Removing its public job immediately is safe: late events are ignored.
            daemon.jobs.cancel(job.id);
            daemon.jobs.jobs.remove(&job.id);
            match fs::remove_file(output) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(operation_error(error)),
            }
            ok()
        }
        Op::ClaimStartup(_) => Ok(Reply::Startup(pb::StartupResult {
            run: !shared.startup_claimed.swap(true, Ordering::AcqRel),
        })),
        Op::StartupFile(path) => {
            daemon.set_startup_file(PathBuf::from(path.path));
            ok()
        }
        Op::Session(_) => Ok(Reply::Session(daemon.session_info().into())),
        Op::LocalCwd(_) => {
            let path = daemon
                .local_directory
                .to_str()
                .ok_or_else(|| operation_error("Local CWD is not UTF-8"))?;
            Ok(Reply::LocalCwd(pb::Path { path: path.into() }))
        }
        Op::SetLocalCwd(path) => {
            daemon
                .set_local_directory(PathBuf::from(path.path))
                .map_err(operation_error)?;
            ok()
        }
        Op::Command(command) => command_reply(crate::commands::dispatch_daemon_result(
            &mut daemon,
            &command.line,
        )),
        Op::ConnectPassword(mut credentials) => {
            let target = Zeroizing::new(std::mem::take(&mut credentials.target));
            let password =
                crate::credentials::Password::new(std::mem::take(&mut credentials.password));
            command_reply(crate::commands::dispatch_connect_result(
                &mut daemon,
                &target,
                Some(password),
            ))
        }
        Op::Job(job) => Ok(Reply::Job(job_snapshot(
            daemon.jobs.jobs.get(&job.id).ok_or_else(missing)?,
        ))),
        Op::Jobs(request) => {
            // Retain only a bounded page of the lowest IDs after the cursor.
            let limit = if request.limit == 0 {
                ipc::JOB_PAGE
            } else {
                (request.limit as usize).min(ipc::JOB_PAGE)
            };
            let mut ids = std::collections::BinaryHeap::new();
            let mut total = 0usize;
            for &id in daemon.jobs.jobs.keys().filter(|&&id| id > request.after_id) {
                total += 1;
                ids.push(id);
                if ids.len() > limit {
                    ids.pop();
                }
            }
            use prost::Message;
            let mut jobs = Vec::new();
            let mut bytes = 64usize; // envelope/cursor/length overhead
            for id in ids.into_sorted_vec() {
                let job = job_snapshot(&daemon.jobs.jobs[&id]);
                let size = job.encoded_len().saturating_add(16);
                if bytes.saturating_add(size) > ipc::MAX_FRAME {
                    if jobs.is_empty() {
                        return Err(rpc_error(
                            pb::ErrorCode::ResponseTooLarge,
                            "job metadata exceeds IPC frame limit",
                        ));
                    }
                    break;
                }
                bytes += size;
                jobs.push(job);
            }
            let next_after_id = if total > jobs.len() {
                jobs.last().map(|job| job.id)
            } else {
                None
            };
            Ok(Reply::Jobs(pb::JobsResult {
                jobs,
                next_after_id,
            }))
        }
        Op::ReadOutput(request) => {
            let job = daemon.jobs.jobs.get(&request.job_id).ok_or_else(missing)?;
            let length = match fs::metadata(job.output.path()) {
                Ok(value) => value.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(operation_error(error)),
            };
            if request.offset > length {
                return Err(rpc_error(
                    pb::ErrorCode::InvalidRequest,
                    "output offset exceeds stored output",
                ));
            }
            let data = job
                .output
                .read_bytes(request.offset)
                .map_err(operation_error)?;
            let next_offset = request
                .offset
                .checked_add(data.len() as u64)
                .ok_or_else(|| operation_error("output offset overflow"))?;
            // A running retry may truncate its spool between stat and read.
            // A short read observed EOF even if the earlier length is stale;
            // never emit an empty non-EOF chunk that makes clients spin/fail.
            let eof = data.len() < ipc::OUTPUT_CHUNK || next_offset == length;
            Ok(Reply::Output(pb::OutputChunk {
                next_offset,
                data,
                eof,
            }))
        }
        Op::AcknowledgeOutput(job) => {
            let value = daemon.jobs.jobs.get(&job.id).ok_or_else(missing)?;
            if value.state == JobState::Running {
                return Err(operation_error(
                    "cannot acknowledge output of a running job",
                ));
            }
            match fs::remove_file(value.output.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(operation_error(error)),
            }
            daemon.jobs.jobs.remove(&job.id);
            ok()
        }
        Op::Cancel(job) => {
            if !daemon.jobs.jobs.contains_key(&job.id) {
                return Err(missing());
            }
            daemon.jobs.cancel(job.id);
            ok()
        }
        Op::Settings(request) => {
            use pb::setting::Value;
            let mut names = daemon
                .settings
                .values
                .keys()
                .filter(|name| request.name.as_ref().is_none_or(|wanted| wanted == *name))
                .collect::<Vec<_>>();
            names.sort_unstable();
            if names.is_empty() && request.name.is_some() {
                return Err(rpc_error(pb::ErrorCode::NotFound, "unknown setting"));
            }
            let mut settings = Vec::new();
            let mut bytes = 64usize;
            for name in names {
                let setting = &daemon.settings.values[name];
                // Check textual sizes before copying, including values loaded
                // from configuration rather than from bounded RPC requests.
                let value_size = match &setting.value {
                    SettingValue::Text(value) => value.len(),
                    _ => 16,
                };
                let size = name
                    .len()
                    .saturating_add(setting.doc.len())
                    .saturating_add(value_size)
                    .saturating_add(32);
                if bytes.saturating_add(size) > ipc::MAX_FRAME {
                    return Err(rpc_error(
                        pb::ErrorCode::ResponseTooLarge,
                        "settings response is too large; request a specific setting",
                    ));
                }
                bytes += size;
                let value = match &setting.value {
                    SettingValue::Bool(value) => Value::Boolean(*value),
                    SettingValue::Int(value) => Value::Integer(*value),
                    SettingValue::Text(value) => Value::Text(value.clone()),
                    SettingValue::Secret(value) => Value::SecretIsSet(value.is_some()),
                };
                settings.push(pb::Setting {
                    name: name.clone(),
                    description: setting.doc.into(),
                    value: Some(value),
                });
            }
            Ok(Reply::Settings(pb::SettingsResult { settings }))
        }
        _ => Err(rpc_error(
            pb::ErrorCode::InvalidRequest,
            "invalid operation in attached session",
        )),
    }
}

fn job_snapshot(job: &crate::core::Job) -> crate::ipc::pb::Job {
    use crate::{core::Progress, ipc::pb};
    let elapsed_ms = job.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let progress = match job.progress.borrow().clone() {
        Progress::Indeterminate { message } => {
            pb::job::Progress::Indeterminate(pb::Indeterminate { message })
        }
        Progress::Determinate {
            done,
            total,
            message,
        } => pb::job::Progress::Determinate(pb::Determinate {
            done,
            total,
            message,
        }),
        Progress::Transfer(mut value) => {
            value.elapsed_ms = elapsed_ms;
            pb::job::Progress::Transfer((&value).into())
        }
    };
    let state = match job.state {
        JobState::Running => pb::JobState::Running,
        JobState::Completed => pb::JobState::Completed,
        JobState::Failed => pb::JobState::Failed,
        JobState::Cancelled => pb::JobState::Cancelled,
    } as i32;
    pb::Job {
        id: job.id,
        title: job.title.clone(),
        state,
        raw_output: job.raw_output,
        elapsed_ms,
        progress: Some(progress),
    }
}

fn handle_completion(
    shared: &Shared,
    request: crate::ipc::pb::CompletionRequest,
    runtime: &tokio::runtime::Handle,
) -> RpcResult {
    use crate::ipc::{pb, Reply};
    let kind = pb::CompletionKind::try_from(request.kind)
        .map_err(|_| rpc_error(pb::ErrorCode::InvalidRequest, "unknown completion kind"))?;
    let result = match kind {
        pb::CompletionKind::LocalFile | pb::CompletionKind::LocalDirectory => {
            let directory = shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .local_directory
                .clone();
            if kind == pb::CompletionKind::LocalFile {
                crate::local_path::complete_files(&directory, &request.input)
            } else {
                crate::local_path::complete_directories(&directory, &request.input)
            }
        }
        pb::CompletionKind::Bookmark => shared
            .daemon
            .lock()
            .expect("daemon state poisoned")
            .bookmark_completion(&request.input),
        pb::CompletionKind::RemoteDirectory | pb::CompletionKind::RemotePath => {
            let future = shared
                .daemon
                .lock()
                .expect("daemon state poisoned")
                .path_completion(request.input, kind == pb::CompletionKind::RemoteDirectory);
            runtime.block_on(future)
        }
        _ => {
            return Err(rpc_error(
                pb::ErrorCode::InvalidRequest,
                "missing completion kind",
            ))
        }
    };
    let result = result.map_err(operation_error)?;
    Ok(Reply::Completion(pb::CompletionResult {
        candidates: result.candidates,
        truncated: result.truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ConnectOptions, Connection};
    use crate::commands::cd::snapshot_directory;
    use async_trait::async_trait;

    #[test]
    fn sockets_are_unique_even_with_the_same_pid_and_metadata_is_complete() {
        let runtime = tempfile::tempdir().unwrap();
        let mut first = DaemonSocket::bind_in(runtime.path()).unwrap();
        let mut second = DaemonSocket::bind_in(runtime.path()).unwrap();
        assert_ne!(first.id, second.id);
        first.publish().unwrap();
        second.publish().unwrap();
        let first_metadata = first.metadata.clone().unwrap();
        let first_directory = first.directory.path().to_owned();
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&first_metadata).unwrap()).unwrap();
        assert_eq!(metadata, first.description().unwrap());
        let mut announcement = Vec::new();
        first.announce(&mut announcement).unwrap();
        assert_eq!(announcement.last(), Some(&b'\n'));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&announcement).unwrap(),
            metadata
        );
        assert_eq!(
            fs::metadata(&first_metadata).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&first_directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(first_directory.join("s"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(first);
        assert!(!first_metadata.exists());
        assert!(!first_directory.exists());
        assert!(second.metadata.as_ref().unwrap().exists());
        assert!(UnixStream::connect(second.directory.path().join("s")).is_ok());
    }

    #[test]
    fn metadata_publication_never_overwrites_an_existing_entry() {
        let runtime = tempfile::tempdir().unwrap();
        let mut socket = DaemonSocket::bind_in(runtime.path()).unwrap();
        let metadata = runtime.path().join(format!("{}.meta", socket.id));
        fs::write(&metadata, "existing entry").unwrap();
        assert!(socket.publish().is_err());
        drop(socket);
        assert_eq!(fs::read_to_string(metadata).unwrap(), "existing entry");
        assert_eq!(fs::read_dir(runtime.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_readiness_announcement_cleans_up_socket_and_metadata() {
        struct BrokenPipe;
        impl Write for BrokenPipe {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let runtime = tempfile::tempdir().unwrap();
        let mut socket = DaemonSocket::bind_in(runtime.path()).unwrap();
        socket.publish().unwrap();
        assert!(socket.announce(&mut BrokenPipe).is_err());
        drop(socket);
        assert_eq!(fs::read_dir(runtime.path()).unwrap().count(), 0);
    }

    struct FakeBackend;
    struct FakeConnection;
    struct NavigationConnection {
        cwd: String,
    }
    #[async_trait]
    impl Connection for NavigationConnection {
        fn protocol(&self) -> &'static str {
            "fake"
        }
        fn pwd(&self) -> Result<String> {
            Ok(self.cwd.clone())
        }
        fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
            Ok(match path {
                None => "/".into(),
                Some(path) if path.starts_with('/') => path.to_owned(),
                Some(path) => format!("{}{}/", self.cwd, path.trim_end_matches('/')),
            })
        }
        async fn change_directory(&mut self, path: &str) -> Result<()> {
            self.cwd = path.to_owned();
            Ok(())
        }
    }

    #[async_trait]
    impl Connection for FakeConnection {
        fn protocol(&self) -> &'static str {
            "fake"
        }
    }
    #[async_trait]
    impl Backend for FakeBackend {
        fn protocol(&self) -> &'static str {
            "fake"
        }
        fn register_settings(&self, _: &mut Settings) {}
        async fn connect(&self, _: ConnectOptions) -> Result<Box<dyn Connection>> {
            Ok(Box::new(FakeConnection))
        }
    }
    async fn finish(daemon: &mut Daemon) {
        for _ in 0..100 {
            if !daemon.has_running_jobs() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("job failed to finish");
    }
    #[test]
    fn directory_snapshot_does_not_drift_between_retries() {
        let mut connection = NavigationConnection {
            cwd: "/original/".into(),
        };
        let snapshot = std::sync::Mutex::new(None);
        assert_eq!(
            snapshot_directory(&connection, Some("photos"), &snapshot).unwrap(),
            "/original/photos/"
        );
        connection.cwd = "/changed/".into();
        assert_eq!(
            snapshot_directory(&connection, Some("photos"), &snapshot).unwrap(),
            "/original/photos/"
        );
    }

    #[tokio::test]
    async fn daemon_cd_and_pwd_use_backend_session_state() {
        let mut daemon = Daemon::bootstrap().unwrap();
        *daemon.connection.lock().await = Some(Box::new(NavigationConnection { cwd: "/".into() }));
        assert!(daemon.command("cd photos")[0].starts_with("JOB "));
        finish(&mut daemon).await;
        assert_eq!(
            daemon
                .connection
                .lock()
                .await
                .as_ref()
                .unwrap()
                .pwd()
                .unwrap(),
            "/photos/"
        );
        daemon.command("pwd");
        finish(&mut daemon).await;
        assert_eq!(daemon.jobs.jobs[&2].output.read_all(), vec!["/photos/"]);
        daemon.command("cd");
        finish(&mut daemon).await;
        assert_eq!(
            daemon
                .connection
                .lock()
                .await
                .as_ref()
                .unwrap()
                .pwd()
                .unwrap(),
            "/"
        );
    }

    #[test]
    fn config_status_distinguishes_missing_and_loaded_files() {
        let dir = std::env::temp_dir().join(format!("xfer-config-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("xfer.conf");
        let mut settings = Settings::new();
        settings.register("test:value", SettingValue::Text("default".into()), "test");
        let missing = ConfigFile::load(&mut settings, path.clone()).unwrap();
        assert!(!missing.loaded);
        assert_eq!(
            missing.status("Config file"),
            format!(
                "Config file: {} (not loaded: file not found)",
                path.display()
            )
        );
        assert_eq!(settings.text("test:value"), Some("default"));

        fs::write(&path, "test:value: configured\n").unwrap();
        let loaded = ConfigFile::load(&mut settings, path.clone()).unwrap();
        assert!(loaded.loaded);
        assert_eq!(
            loaded.status("Config file"),
            format!("Config file: {}", path.display())
        );
        assert_eq!(settings.text("test:value"), Some("configured"));

        let override_path = dir.join("override.conf");
        fs::write(&override_path, "test:value: overridden\n").unwrap();
        ConfigFile::load(&mut settings, override_path).unwrap();
        assert_eq!(settings.text("test:value"), Some("overridden"));
        fs::remove_dir_all(dir).unwrap();
        // Status describes what was loaded at startup, not whether it still exists.
        assert!(!loaded.status("Config file").contains("not loaded"));
    }

    #[test]
    fn status_shows_config_paths_and_current_bookmarks() {
        let mut daemon = Daemon::bootstrap().unwrap();
        let config = std::path::absolute(crate::config_dir().join("xfer.conf")).unwrap();
        assert_eq!(daemon.config_file.path, config);
        assert_eq!(
            daemon.config_override.as_ref().map(|config| &config.path),
            std::env::var_os("XFER_CONFIG")
                .map(std::path::absolute)
                .transpose()
                .unwrap()
                .as_ref()
        );
        let expected_config = daemon.config_file.status("Config file");
        let status = daemon.command("status");
        assert!(status.contains(&expected_config));
        assert!(!status.iter().any(|line| line.starts_with("RC file:")));

        let override_path = std::path::absolute("custom config.conf").unwrap();
        daemon.config_override = Some(ConfigFile {
            path: override_path.clone(),
            loaded: true,
        });
        let rc = std::path::absolute(".xferrc").unwrap();
        daemon.set_startup_file(rc.clone());
        daemon.command("set bookmarks:file 'custom bookmarks.toml'");
        // Client reattachment may change the download directory, not config paths.
        daemon.set_local_directory(std::env::temp_dir()).unwrap();
        let status = daemon.command("status");
        assert!(status.contains(&expected_config));
        assert!(status.contains(&format!(
            "Config override (XFER_CONFIG): {}",
            override_path.display()
        )));
        assert!(status.contains(&format!("RC file: {}", rc.display())));
        assert!(status.contains(&format!(
            "Bookmarks file: {}",
            std::path::absolute("custom bookmarks.toml")
                .unwrap()
                .display()
        )));
        daemon.command("set bookmarks:file other.toml");
        assert!(daemon.command("status").contains(&format!(
            "Bookmarks file: {}",
            std::path::absolute("other.toml").unwrap().display()
        )));
    }

    #[test]
    fn prompt_setting_refreshes_from_committed_state() {
        let mut daemon = Daemon::bootstrap().unwrap();
        daemon.settings.set("ui:prompt", "xfer$location> ").unwrap();
        assert_eq!(daemon.session_info().prompt.as_deref(), Some("xfer> "));
        *daemon.session.lock().unwrap() = SessionInfo {
            protocol: Some("sftp".into()),
            cwd: Some("/home/antek/".into()),
            user: "antek".into(),
            host: "example.org".into(),
            ..Default::default()
        };
        assert_eq!(
            daemon.session_info().prompt.as_deref(),
            Some("xfer[sftp:/home/antek/]> ")
        );
        daemon.command("set ui:prompt '$user@$host [$cwdn]> '");
        assert_eq!(
            daemon.session_info().prompt.as_deref(),
            Some("antek@example.org [antek]> ")
        );
        *daemon.session.lock().unwrap() = SessionInfo::disconnected();
        assert_eq!(daemon.session_info().prompt.as_deref(), Some("@ []> "));
    }

    #[tokio::test]
    async fn status_snapshot_tracks_cwd_close_and_does_not_wait_on_connection_lock() {
        let mut daemon = Daemon::bootstrap().unwrap();
        assert!(daemon.session_info().protocol.is_none());
        *daemon.connection.lock().await = Some(Box::new(NavigationConnection { cwd: "/".into() }));
        daemon.command("cd photos");
        finish(&mut daemon).await;
        let info = daemon.session_info();
        assert_eq!(info.daemon_pid, std::process::id());
        assert_eq!(info.protocol.as_deref(), Some("fake"));
        assert_eq!(info.cwd.as_deref(), Some("/photos/"));
        let connection = daemon.connection.clone();
        let _busy = connection.lock().await;
        let status = daemon.command("status");
        assert!(status.contains(&format!("Daemon PID: {}", std::process::id())));
        assert!(status.contains(&"CWD: /photos/".into()));
        drop(_busy);
        daemon.command("close");
        finish(&mut daemon).await;
        assert!(daemon.session_info().protocol.is_none());
        assert!(daemon.session_info().cwd.is_none());
    }

    #[tokio::test]
    async fn daemon_accepts_quoted_directory_paths() {
        let mut daemon = Daemon::bootstrap().unwrap();
        *daemon.connection.lock().await = Some(Box::new(NavigationConnection { cwd: "/".into() }));
        daemon.command("cd 'photo album'");
        finish(&mut daemon).await;
        assert_eq!(
            daemon
                .connection
                .lock()
                .await
                .as_ref()
                .unwrap()
                .pwd()
                .unwrap(),
            "/photo album/"
        );
    }

    #[tokio::test]
    async fn acknowledging_failed_output_removes_job_and_spool() {
        let mut daemon = Daemon::bootstrap().unwrap();
        let id = daemon
            .jobs
            .start_operation("failure".into(), async { anyhow::bail!("test failure") });
        assert_eq!(
            daemon.command(&format!("ack-output {id}")),
            vec!["cannot acknowledge output of a running job"]
        );
        finish(&mut daemon).await;
        let path = daemon.jobs.jobs[&id].output.path().to_owned();
        assert!(path.exists());
        assert_eq!(
            daemon.command(&format!("ack-output {id}")),
            vec!["output acknowledged"]
        );
        assert!(!daemon.jobs.jobs.contains_key(&id));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn close_releases_active_connection() {
        let mut daemon = Daemon::bootstrap().unwrap();
        *daemon.connection.lock().await = Some(Box::new(FakeConnection));
        assert!(daemon.command("close")[0].starts_with("JOB "));
        finish(&mut daemon).await;
        assert!(daemon.connection.lock().await.is_none());
        // Closing an already closed connection is harmless.
        assert!(daemon.command("close")[0].starts_with("JOB "));
        finish(&mut daemon).await;
        assert!(daemon.connection.lock().await.is_none());
    }

    #[tokio::test]
    async fn open_is_an_alias_for_connect() {
        let mut daemon = Daemon::bootstrap().unwrap();
        daemon
            .backends
            .insert("fake".into(), std::sync::Arc::new(FakeBackend));
        assert!(daemon.command("open fake://host")[0].starts_with("JOB "));
        finish(&mut daemon).await;
        assert_eq!(daemon.session_info().protocol.as_deref(), Some("fake"));
        assert_eq!(
            daemon.connection.lock().await.as_ref().unwrap().protocol(),
            "fake"
        );
        assert_eq!(daemon.command("open"), daemon.command("connect"));
    }

    #[tokio::test]
    async fn protocol_dispatch_and_disconnect_are_backend_independent() {
        let mut daemon = Daemon::bootstrap().unwrap();
        daemon
            .backends
            .insert("fake".into(), std::sync::Arc::new(FakeBackend));
        assert!(daemon.command("connect fake://host")[0].starts_with("JOB "));
        finish(&mut daemon).await;
        assert_eq!(
            daemon.connection.lock().await.as_ref().unwrap().protocol(),
            "fake"
        );
        // Unknown protocols must not replace the working connection.
        assert!(daemon.command("connect unknown://host")[0].contains("no backend"));
        daemon.command("disconnect");
        finish(&mut daemon).await;
        assert!(daemon.connection.lock().await.is_none());
    }
    #[tokio::test]
    async fn attachment_release_cancels_modal_scans_but_not_background_jobs() {
        let daemon = Daemon::bootstrap().unwrap();
        let connection = daemon.connection.clone();
        let _busy = connection.lock().await;
        let shared = Shared {
            attached: Arc::new(AtomicBool::new(true)),
            shutdown: Arc::new(AtomicBool::new(false)),
            startup_claimed: Arc::new(AtomicBool::new(false)),
            daemon: Arc::new(Mutex::new(daemon)),
        };
        let reply = handle_request(
            &shared,
            crate::ipc::Op::StartDirectory(crate::ipc::pb::DirectoryRequest {
                source: crate::ipc::pb::DirectorySource::Remote as i32,
                filter: 0,
            }),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let crate::ipc::Reply::DirectoryStarted(id) = reply else {
            panic!()
        };
        let (slot, background) = {
            let mut daemon = shared.daemon.lock().unwrap();
            let slot = Arc::downgrade(daemon.jobs.jobs[&id.id].directory.as_ref().unwrap());
            let background = daemon
                .jobs
                .start_operation("background".into(), std::future::pending());
            (slot, background)
        };
        tokio::task::yield_now().await;
        drop(AttachmentLease(&shared));
        assert!(!shared.attached.load(Ordering::Acquire));
        assert!(!shared.shutdown.load(Ordering::Acquire));
        {
            let daemon = shared.daemon.lock().unwrap();
            assert!(!daemon.jobs.jobs.contains_key(&id.id));
            assert!(daemon.jobs.jobs.contains_key(&background));
        }
        for _ in 0..20 {
            if slot.upgrade().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(slot.upgrade().is_none());
        shared.daemon.lock().unwrap().jobs.cancel(background);
    }
}
