//! Daemon-owned transfer core. The client must not mutate these structures.

use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;

fn default_history_path() -> String {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".local/state"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    base.join("xfer/history").display().to_string()
}

pub type JobId = u64;

#[derive(Clone, Debug)]
pub enum Progress {
    Indeterminate {
        message: String,
    },
    Transfer(crate::transfer::TransferProgress),
    Determinate {
        done: u64,
        total: u64,
        message: String,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub enum JobState {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Job output is always spooled by the daemon. Clients request ranges from the
/// daemon; they never receive an unbounded completion response.
#[derive(Clone)]
pub struct OutputStore {
    path: PathBuf,
    bytes: u64,
    // Raw remote contents may contain secrets: keep them in a private directory,
    // removed when the final job/operation reference is dropped.
    _directory: Option<std::sync::Arc<tempfile::TempDir>>,
}
impl OutputStore {
    pub(crate) fn new(id: JobId) -> Self {
        let base = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
            .unwrap_or_else(|| PathBuf::from("."));
        // Job IDs are local to a daemon; separate instances must never overwrite each other's output.
        let path = base
            .join("xfer/jobs")
            .join(std::process::id().to_string())
            .join(format!("{id}-{}.log", {
                static NEXT_SPOOL: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(1);
                NEXT_SPOOL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            }));
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::remove_file(&path);
        Self {
            path,
            bytes: 0,
            _directory: None,
        }
    }
    pub(crate) fn temporary() -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("xfer-cat-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let path = directory.path().join("contents");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        Ok(Self {
            path,
            bytes: 0,
            _directory: Some(std::sync::Arc::new(directory)),
        })
    }
    fn append(&mut self, line: &str) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            if writeln!(file, "{line}").is_ok() {
                self.bytes += line.len() as u64 + 1;
            }
        }
    }
    fn reset(&mut self) -> Result<()> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.bytes = 0;
        Ok(())
    }
    pub fn write_line(&mut self, line: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{line}")?;
        self.bytes += line.len() as u64 + 1;
        Ok(())
    }
    pub fn read_bytes(&self, offset: u64) -> Result<Vec<u8>> {
        let mut file = match OpenOptions::new().read(true).open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024).read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    pub fn read_chunk(&self, offset: u64, limit: usize) -> (u64, String) {
        let mut data = vec![0; limit.min(64 * 1024)];
        let read = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .and_then(|mut file| {
                file.seek(SeekFrom::Start(offset))?;
                file.read(&mut data)
            })
            .unwrap_or(0);
        data.truncate(read);
        (
            offset + read as u64,
            String::from_utf8_lossy(&data).into_owned(),
        )
    }
    pub fn read_all(&self) -> Vec<String> {
        fs::read_to_string(&self.path)
            .map(|data| data.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

/// The daemon's text output adapter. Backends know only about typed records;
/// this is the one place where listing entries become terminal/pager text.
impl crate::listing::ListingSink for OutputStore {
    fn emit(&mut self, content: crate::listing::Content) -> Result<()> {
        self.write_line(&crate::listing::render(&content))
    }
}

pub struct Job {
    pub id: JobId,
    pub title: String,
    pub state: JobState,
    cancel: CancellationToken,
    pub progress: watch::Receiver<Progress>,
    pub output: OutputStore,
    pub(crate) raw_output: bool,
    pub(crate) started: time::Instant,
    _handle: JoinHandle<()>,
}

struct JobEvent {
    pub id: JobId,
    event: JobEventKind,
}
enum JobEventKind {
    Output(String),
    Finished(JobState),
}

pub struct JobManager {
    pub jobs: HashMap<JobId, Job>,
    next_id: JobId,
    tx: mpsc::UnboundedSender<JobEvent>,
    rx: mpsc::UnboundedReceiver<JobEvent>,
}

impl JobManager {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            jobs: HashMap::new(),
            next_id: 1,
            tx,
            rx,
        }
    }

    pub fn start_operation<F>(&mut self, title: String, operation: F) -> JobId
    where
        F: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.start_output_operation(title, move |_| operation)
    }

    pub fn start_output_operation<F, Fut>(&mut self, title: String, operation: F) -> JobId
    where
        F: FnOnce(OutputStore) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.start_task(title, move |output, _| operation(output))
    }

    /// Replayable, read-only operations. Partial attempt output is discarded so
    /// retrying a paginated listing never duplicates already written entries.
    pub fn start_retrying_operation<F, Fut>(&mut self, title: String, mut operation: F) -> JobId
    where
        F: FnMut(OutputStore) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.start_task(title, move |mut output, progress| async move {
            let mut delay = Duration::from_secs(3);
            let mut attempt = 1u64;
            loop {
                let _ = progress.send(Progress::Indeterminate {
                    message: format!("attempt {attempt}: working"),
                });
                match operation(output.clone()).await {
                    Ok(message) => return Ok(message),
                    Err(error) => {
                        if error.is::<crate::error::NonRetryableError>() {
                            return Err(error);
                        }
                        let _ = progress.send(Progress::Indeterminate {
                            message: format!(
                                "attempt {attempt} failed: {error}; retrying in {}s",
                                delay.as_secs()
                            ),
                        });
                        // Do not accumulate failure logs indefinitely in memory or on disk.
                        output.reset()?;
                        time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
        })
    }

    pub(crate) fn start_task<F, Fut>(&mut self, title: String, operation: F) -> JobId
    where
        F: FnOnce(OutputStore, watch::Sender<Progress>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.start_task_with_output(title, None, operation)
    }

    pub(crate) fn start_raw_task<F, Fut>(
        &mut self,
        title: String,
        output: OutputStore,
        operation: F,
    ) -> JobId
    where
        F: FnOnce(OutputStore, watch::Sender<Progress>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        self.start_task_with_output(title, Some(output), operation)
    }

    fn start_task_with_output<F, Fut>(
        &mut self,
        title: String,
        output: Option<OutputStore>,
        operation: F,
    ) -> JobId
    where
        F: FnOnce(OutputStore, watch::Sender<Progress>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;
        let raw_output = output.is_some();
        let output = output.unwrap_or_else(|| OutputStore::new(id));
        let task_output = output.clone();
        let (progress_tx, progress) = watch::channel(Progress::Indeterminate {
            message: "working".into(),
        });
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let events = self.tx.clone();
        let handle = tokio::spawn(async move {
            let operation = operation(task_output, progress_tx.clone());
            let (state, message) = tokio::select! {
                biased;
                _ = task_cancel.cancelled() => (JobState::Cancelled, "operation cancelled".to_owned()),
                result = operation => match result { Ok(message) => (JobState::Completed, message), Err(error) => (JobState::Failed, error.to_string()) },
            };
            let _ = progress_tx.send(Progress::Indeterminate {
                message: message.clone(),
            });
            // Empty successes have no footer (ls); raw jobs keep all diagnostics
            // exclusively in status, never mixed into their file bytes.
            if !message.is_empty() && !raw_output {
                let _ = events.send(JobEvent {
                    id,
                    event: JobEventKind::Output(message),
                });
            }
            let _ = events.send(JobEvent {
                id,
                event: JobEventKind::Finished(state),
            });
        });
        self.jobs.insert(
            id,
            Job {
                id,
                title,
                state: JobState::Running,
                cancel,
                progress,
                output,
                raw_output,
                _handle: handle,
                started: time::Instant::now(),
            },
        );
        id
    }

    pub fn start_demo(&mut self, title: String, known_total: bool) -> JobId {
        let id = self.next_id;
        self.next_id += 1;
        let (progress_tx, progress) = watch::channel(Progress::Indeterminate {
            message: "starting".into(),
        });
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let events = self.tx.clone();
        let handle = tokio::spawn(async move {
            let total = 100_u64;
            for done in 0..=total {
                if task_cancel.is_cancelled() {
                    let _ = events.send(JobEvent {
                        id,
                        event: JobEventKind::Output("operation cancelled".into()),
                    });
                    let _ = events.send(JobEvent {
                        id,
                        event: JobEventKind::Finished(JobState::Cancelled),
                    });
                    return;
                }
                let p = if known_total {
                    Progress::Determinate {
                        done,
                        total,
                        message: format!("transferring chunk {done}/{total}"),
                    }
                } else {
                    Progress::Indeterminate {
                        message: format!("listing remote directory (pass {})", done / 10 + 1),
                    }
                };
                let _ = progress_tx.send(p);
                if done % 25 == 0 {
                    let _ = events.send(JobEvent {
                        id,
                        event: JobEventKind::Output(format!("completed checkpoint {done}%")),
                    });
                }
                time::sleep(Duration::from_millis(45)).await;
            }
            let _ = events.send(JobEvent {
                id,
                event: JobEventKind::Output("operation completed successfully".into()),
            });
            let _ = events.send(JobEvent {
                id,
                event: JobEventKind::Finished(JobState::Completed),
            });
        });
        self.jobs.insert(
            id,
            Job {
                id,
                title,
                state: JobState::Running,
                cancel,
                progress,
                output: OutputStore::new(id),
                raw_output: false,
                _handle: handle,
                started: time::Instant::now(),
            },
        );
        id
    }

    pub fn poll(&mut self) -> Vec<JobId> {
        let mut completed = vec![];
        while let Ok(message) = self.rx.try_recv() {
            if let Some(job) = self.jobs.get_mut(&message.id) {
                match message.event {
                    JobEventKind::Output(line) => job.output.append(&line),
                    JobEventKind::Finished(state) => {
                        job.state = state;
                        completed.push(job.id);
                    }
                }
            }
        }
        completed
    }

    pub fn cancel(&self, id: JobId) {
        if let Some(job) = self.jobs.get(&id) {
            job.cancel.cancel();
        }
    }
}

#[derive(Clone, Debug)]
pub enum SettingValue {
    Bool(bool),
    Int(i64),
    Text(String),
}
impl SettingValue {
    pub fn display(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Text(value) => value.clone(),
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(v) = self {
            Some(*v)
        } else {
            None
        }
    }
}
#[derive(Clone)]
pub struct Setting {
    pub default: SettingValue,
    pub value: SettingValue,
    pub doc: &'static str,
}
#[derive(Clone)]
pub struct Settings {
    pub values: HashMap<String, Setting>,
}
impl Settings {
    /// Load settings, returning false when the optional file is missing.
    pub fn load_file(&mut self, path: &std::path::Path) -> Result<bool> {
        let source = match std::fs::read_to_string(path) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        for (line_no, line) in source.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, value) = line
                .split_once(": ")
                .or_else(|| line.rsplit_once(':'))
                .ok_or_else(|| {
                    anyhow::anyhow!("{}:{}: expected `name: value`", path.display(), line_no + 1)
                })?;
            self.set(name.trim(), value.trim())
                .map_err(|e| anyhow::anyhow!("{}:{}: {e}", path.display(), line_no + 1))?;
        }
        Ok(true)
    }
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
    pub fn register(&mut self, name: &str, default: SettingValue, doc: &'static str) {
        self.values.insert(
            name.into(),
            Setting {
                value: default.clone(),
                default,
                doc,
            },
        );
    }
    pub fn bool(&self, name: &str) -> bool {
        self.values
            .get(name)
            .and_then(|v| v.value.as_bool())
            .unwrap_or(false)
    }
    pub fn text(&self, name: &str) -> Option<&str> {
        self.values.get(name).and_then(|v| match &v.value {
            SettingValue::Text(value) => Some(value.as_str()),
            _ => None,
        })
    }
    pub fn set(&mut self, name: &str, raw: &str) -> Result<()> {
        let setting = self
            .values
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("unknown setting: {name}"))?;
        setting.value = match setting.default {
            SettingValue::Bool(_) => SettingValue::Bool(raw.parse()?),
            SettingValue::Int(_) => SettingValue::Int(raw.parse()?),
            SettingValue::Text(_) => SettingValue::Text(raw.into()),
        };
        Ok(())
    }
}
pub use crate::backend::{Backend, Sftp, Smb, S3};

/// Lightweight committed session state: safe to query while a job owns the
/// backend connection lock. It never contains credentials or endpoint secrets.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SessionInfo {
    pub daemon_pid: u32,
    pub protocol: Option<String>,
    pub cwd: Option<String>,
}
impl SessionInfo {
    pub(crate) fn disconnected() -> Self {
        Self {
            daemon_pid: std::process::id(),
            protocol: None,
            cwd: None,
        }
    }
    pub(crate) fn connected(connection: &dyn crate::backend::Connection) -> Self {
        Self {
            daemon_pid: std::process::id(),
            protocol: Some(connection.protocol().to_owned()),
            cwd: connection.pwd().ok(),
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
        let backends: Vec<Box<dyn Backend>> = vec![Box::new(Sftp), Box::new(S3), Box::new(Smb)];
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
        self.session.lock().expect("session state poisoned").clone()
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

#[cfg(test)]
mod backend_tests {
    use super::*;
    use crate::backend::{ConnectOptions, Connection};
    use crate::commands::cd::snapshot_directory;
    use async_trait::async_trait;

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

    #[tokio::test(start_paused = true)]
    async fn nonexistent_directories_are_not_retried() {
        let mut jobs = JobManager::new();
        let id = jobs.start_retrying_operation("cd missing".into(), |_| async {
            Err(crate::error::NonRetryableError("no such directory".into()).into())
        });
        tokio::task::yield_now().await;
        jobs.poll();
        assert_eq!(jobs.jobs[&id].state, JobState::Failed);
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

    #[test]
    fn listing_sink_renders_typed_records_to_spool() {
        use crate::listing::{Content, DirectoryInfo, FileInfo, ListingSink};
        let mut spool = OutputStore::new(1);
        let directory = Content::Directory(DirectoryInfo {
            unix: None,
            size: None,
            name: "src".into(),
            modified: None,
        });
        let file = Content::File(FileInfo {
            unix: None,
            name: "notes.txt".into(),
            size: Some(4096),
            modified: None,
        });
        spool.emit(directory.clone()).unwrap();
        spool.emit(file.clone()).unwrap();
        assert_eq!(
            spool.read_all(),
            vec![
                crate::listing::render(&directory),
                crate::listing::render(&file)
            ]
        );
        fs::remove_file(spool.path()).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retry_backoff_caps_at_30_seconds_and_cancels_during_wait() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let mut jobs = JobManager::new();
        let count = Arc::new(AtomicUsize::new(0));
        let attempts = count.clone();
        let id = jobs.start_retrying_operation("retry".into(), move |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { anyhow::bail!("offline") }
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        for delay in [3, 6, 12, 24, 30, 30] {
            let previous = count.load(Ordering::SeqCst);
            time::advance(Duration::from_secs(delay - 1)).await;
            tokio::task::yield_now().await;
            assert_eq!(count.load(Ordering::SeqCst), previous);
            time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(count.load(Ordering::SeqCst), previous + 1);
        }
        jobs.cancel(id);
        tokio::task::yield_now().await;
        jobs.poll();
        assert_eq!(jobs.jobs[&id].state, JobState::Cancelled);
        let previous = count.load(Ordering::SeqCst);
        time::advance(Duration::from_secs(120)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), previous);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_discards_partial_listing_output_before_success() {
        let mut jobs = JobManager::new();
        let mut attempt = 0;
        let id = jobs.start_retrying_operation("ls".into(), move |mut output| {
            attempt += 1;
            let current = attempt;
            async move {
                if current == 1 {
                    output.write_line("partial listing")?;
                    anyhow::bail!("interrupted");
                }
                output.write_line("complete listing")?;
                Ok("done".to_owned())
            }
        });
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        jobs.poll();
        assert_eq!(jobs.jobs[&id].state, JobState::Completed);
        assert_eq!(
            jobs.jobs[&id].output.read_all(),
            vec!["complete listing", "done"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_in_flight_retry_operation_is_immediate() {
        let mut jobs = JobManager::new();
        let id = jobs.start_retrying_operation("blocked".into(), |_| async {
            std::future::pending::<Result<String>>().await
        });
        tokio::task::yield_now().await;
        jobs.cancel(id);
        tokio::task::yield_now().await;
        jobs.poll();
        assert_eq!(jobs.jobs[&id].state, JobState::Cancelled);
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
