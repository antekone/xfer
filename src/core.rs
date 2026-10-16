//! Daemon-owned transfer core. The client must not mutate these structures.

use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
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
        Self { path, bytes: 0 }
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

pub struct Job {
    pub id: JobId,
    pub title: String,
    pub state: JobState,
    cancel: CancellationToken,
    pub progress: watch::Receiver<Progress>,
    pub output: OutputStore,
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

    fn start_task<F, Fut>(&mut self, title: String, operation: F) -> JobId
    where
        F: FnOnce(OutputStore, watch::Sender<Progress>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;
        let output = OutputStore::new(id);
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
            let _ = events.send(JobEvent {
                id,
                event: JobEventKind::Output(message),
            });
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
                _handle: handle,
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
                _handle: handle,
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
    pub fn load_file(&mut self, path: &std::path::Path) -> Result<()> {
        let source = match std::fs::read_to_string(path) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
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
        Ok(())
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

/// Authoritative daemon state. Clients interact with it through IPC and do not
/// own jobs, connections, or backend configuration.
pub struct Daemon {
    pub settings: Settings,
    pub jobs: JobManager,
    backends: HashMap<String, std::sync::Arc<dyn Backend>>,
    connection: std::sync::Arc<tokio::sync::Mutex<Option<Box<dyn crate::backend::Connection>>>>,
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
        settings.load_file(std::path::Path::new("xfer.conf"))?;
        if let Some(path) = std::env::var_os("XFER_CONFIG") {
            settings.load_file(std::path::Path::new(&path))?;
        }
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

    fn connect(&mut self, target: &str) -> Result<JobId> {
        let bookmark = if target.contains("://") {
            None
        } else {
            Some(
                crate::bookmarks::load(std::path::Path::new(
                    self.settings.text("bookmarks:file").unwrap(),
                ))?
                .remove(target)
                .ok_or_else(|| anyhow::anyhow!("unknown bookmark: {target}"))?,
            )
        };
        let token = bookmark
            .as_ref()
            .map(|value| value.protocol.as_str())
            .or_else(|| target.split_once("://").map(|(token, _)| token))
            .unwrap_or("");
        let backend = self
            .backends
            .get(token)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no backend for protocol: {token}"))?;
        let options = crate::backend::ConnectOptions {
            target: target.to_owned(),
            bookmark,
            settings: self.settings.clone(),
        };
        let connection = self.connection.clone();
        Ok(self
            .jobs
            .start_retrying_operation(format!("connect {target}"), move |_| {
                let connection = connection.clone();
                let backend = backend.clone();
                let options = options.clone();
                async move {
                    // Serialize changes per attempt; the lock is released during backoff.
                    let mut active = connection.lock().await;
                    let new = backend.connect(options).await?;
                    let message = format!("connected {}", new.protocol());
                    *active = Some(new);
                    Ok(message)
                }
            }))
    }

    pub fn command(&mut self, line: &str) -> Vec<String> {
        self.jobs.poll();
        let mut words = line.split_whitespace();
        match words.next() {
            Some("help") => vec![
                "Commands:".into(),
                "  connect <url|name>  connect using a protocol or bookmark".into(),
                "  open <url|name>     alias for connect".into(),
                "  disconnect          release active backend connection".into(),
                "  close               alias for disconnect".into(),
                "  ls [prefix]         list buckets or selected bucket contents".into(),
                "  bookmark            list bookmark aliases, URLs and buckets".into(),
                "  bookmark edit       edit TOML bookmarks using VISUAL/EDITOR".into(),
                "  jobs                list daemon jobs".into(),
                "  cancel <job-id>     cancel a job".into(),
                "  show [setting]      show daemon settings".into(),
                "  set <name> <value>  update a daemon setting".into(),
                "  detach              detach this client".into(),
                "  exit                detach; daemon remains only while jobs run".into(),
            ],
            Some("bookmark") => match words.next() {
                None => match crate::bookmarks::load(std::path::Path::new(
                    self.settings.text("bookmarks:file").unwrap(),
                )) {
                    Ok(entries) if entries.is_empty() => vec!["no bookmarks".into()],
                    Ok(entries) => entries
                        .into_iter()
                        .map(|(alias, bookmark)| bookmark.summary(&alias))
                        .collect(),
                    Err(error) => vec![error.to_string()],
                },
                Some("edit") => {
                    let path = std::path::Path::new(self.settings.text("bookmarks:file").unwrap());
                    match crate::bookmarks::prepare_for_editor(path) {
                        Ok(()) => vec![format!("BOOKMARK_EDIT {}", path.display())],
                        Err(error) => vec![format!("cannot prepare bookmarks: {error}")],
                    }
                }
                Some("reload") => match crate::bookmarks::load(std::path::Path::new(
                    self.settings.text("bookmarks:file").unwrap(),
                )) {
                    Ok(_) => vec!["bookmarks validated; connections use the latest file".into()],
                    Err(error) => vec![error.to_string()],
                },
                _ => vec!["usage: bookmark [edit | reload]".into()],
            },
            Some("connect" | "open") => match words.next() {
                Some(target) => match self.connect(target) {
                    Ok(id) => vec![format!("JOB {id}")],
                    Err(error) => vec![error.to_string()],
                },
                None => vec!["usage: connect <protocol://target | bookmark>".into()],
            },
            Some("ls") => {
                let path = words.next().unwrap_or("").to_owned();
                if words.next().is_some() {
                    return vec!["usage: ls [prefix]".into()];
                }
                let connection = self.connection.clone();
                let id =
                    self.jobs
                        .start_retrying_operation(format!("ls {path}"), move |mut output| {
                            let connection = connection.clone();
                            let path = path.clone();
                            async move {
                                let active = connection.lock().await;
                                let active = active.as_ref().ok_or_else(|| {
                                    anyhow::anyhow!("not connected; use connect first")
                                })?;
                                let count = active.list(&path, &mut output).await?;
                                Ok(format!("listed {count} entries"))
                            }
                        });
                vec![format!("JOB {id}")]
            }
            Some("disconnect" | "close") => {
                let connection = self.connection.clone();
                let id = self.jobs.start_operation("disconnect".into(), async move {
                    let previous = connection.lock().await.take();
                    match previous {
                        Some(value) => {
                            let token = value.protocol();
                            value.disconnect().await?;
                            Ok(format!("disconnected {token}"))
                        }
                        None => Ok("not connected".into()),
                    }
                });
                vec![format!("JOB {id}")]
            }
            Some("get" | "mirror") => vec!["file transfers are not implemented yet".into()],
            Some("read-output") => {
                let id = words.next().and_then(|value| value.parse().ok());
                let offset = words
                    .next()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                match id.and_then(|id| self.jobs.jobs.get(&id)) {
                    Some(job) => match job.output.read_bytes(offset) {
                        Ok(bytes) => vec![format!(
                            "CHUNK {} {}",
                            offset + bytes.len() as u64,
                            serde_json::to_string(&bytes).unwrap()
                        )],
                        Err(error) => vec![format!("cannot read job output: {error}")],
                    },
                    None => vec!["no such job".into()],
                }
            }
            Some("output") => {
                let id = words.next().and_then(|value| value.parse().ok());
                let offset = words
                    .next()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                match id.and_then(|id| self.jobs.jobs.get(&id)) {
                    Some(job) => {
                        let (next, data) = job.output.read_chunk(offset, 64 * 1024);
                        let mut lines = vec![format!("OFFSET {next}")];
                        lines.extend(data.lines().map(str::to_owned));
                        lines
                    }
                    None => vec!["no such job".into()],
                }
            }
            Some("ack-output") => match words.next().and_then(|value| value.parse().ok()) {
                Some(id) => match self.jobs.jobs.get(&id) {
                    Some(job) if job.state == JobState::Running => {
                        vec!["cannot acknowledge output of a running job".into()]
                    }
                    Some(job) => match fs::remove_file(job.output.path()) {
                        Ok(()) => {
                            self.jobs.jobs.remove(&id);
                            vec!["output acknowledged".into()]
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            self.jobs.jobs.remove(&id);
                            vec!["output acknowledged".into()]
                        }
                        Err(error) => vec![format!("cannot remove output spool: {error}")],
                    },
                    None => vec!["no such job".into()],
                },
                None => vec!["usage: ack-output <job-id>".into()],
            },
            Some("status") => match words
                .next()
                .and_then(|value| value.parse().ok())
                .and_then(|id| self.jobs.jobs.get(&id))
            {
                Some(job) => match job.progress.borrow().clone() {
                    Progress::Determinate {
                        done,
                        total,
                        message,
                    } => vec![format!(
                        "[{}] {:?} {done}/{total} {message}",
                        job.id, job.state
                    )],
                    Progress::Indeterminate { message } => {
                        vec![format!("[{}] {:?} {message}", job.id, job.state)]
                    }
                },
                None => vec!["no such job".into()],
            },
            Some("jobs") => self
                .jobs
                .jobs
                .values()
                .map(|job| format!("[{}] {:?} {}", job.id, job.state, job.title))
                .collect(),
            Some("cancel") => match words.next().and_then(|value| value.parse().ok()) {
                Some(id) => {
                    self.jobs.cancel(id);
                    vec![format!("cancel requested for job {id}")]
                }
                None => vec!["usage: cancel <job-id>".into()],
            },
            Some("show") => match words.next() {
                Some(name) => match self.settings.values.get(name) {
                    Some(setting) => vec![format!(
                        "{name}: {} — {}",
                        setting.value.display(),
                        setting.doc
                    )],
                    None => vec![format!("unknown setting: {name}")],
                },
                None => {
                    let mut names = self.settings.values.keys().collect::<Vec<_>>();
                    names.sort_unstable();
                    names
                        .into_iter()
                        .map(|name| {
                            let setting = &self.settings.values[name];
                            format!("{name}: {} — {}", setting.value.display(), setting.doc)
                        })
                        .collect()
                }
            },
            Some("set") => match (words.next(), words.next()) {
                (Some(name), Some(value)) => match self.settings.set(name, value) {
                    Ok(()) => vec![format!("{name} set to {value}")],
                    Err(error) => vec![error.to_string()],
                },
                _ => vec!["usage: set <name> <value>".into()],
            },
            _ => vec!["unknown daemon command".into()],
        }
    }
}

#[cfg(test)]
mod backend_tests {
    use super::*;
    use crate::backend::{ConnectOptions, Connection};
    use async_trait::async_trait;

    struct FakeBackend;
    struct FakeConnection;
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
