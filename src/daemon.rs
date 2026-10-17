//! Daemon-owned application state and local per-session IPC server.

// ------------------------------------------------------------------------------------------------

use std::{
    collections::HashMap,
    env, fs,
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

use crate::{
    backend::{Backend, Sftp, Smb, Webdav, S3},
    core::{JobManager, JobState, SettingValue, Settings},
};
use anyhow::{Context, Result};
use zeroize::{Zeroize, Zeroizing};

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

/// Request prefixes answered from the completion cache or the local filesystem.
const COMPLETION_PREFIXES: [&str; 6] = [
    "COMPLETE_CD ",
    "COMPLETE_LS ",
    "COMPLETE_GET ",
    "COMPLETE_BOOKMARK ",
    "COMPLETE_LCD ",
    "COMPLETE_PUT ",
];

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
}
