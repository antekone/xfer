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
    fn new(id: JobId) -> Self {
        let base = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
            .unwrap_or_else(|| PathBuf::from("."));
        let path = base.join("xfer/jobs").join(format!("{id}.log"));
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
pub struct Setting {
    pub default: SettingValue,
    pub value: SettingValue,
    pub doc: &'static str,
}
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
            let (name, value) = line.rsplit_once(':').ok_or_else(|| {
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
pub trait Backend {
    fn register_settings(&self, settings: &mut Settings);
    fn initialize(&self, _settings: &Settings) -> Result<()>;
}
pub struct Sftp;
pub struct S3;
pub struct Smb;
impl Backend for Sftp {
    fn register_settings(&self, s: &mut Settings) {
        s.register(
            "sftp:connect-timeout",
            SettingValue::Int(30),
            "SFTP connection timeout (seconds)",
        );
    }
    fn initialize(&self, _: &Settings) -> Result<()> {
        Ok(())
    }
}
impl Backend for S3 {
    fn register_settings(&self, s: &mut Settings) {
        s.register(
            "s3:region",
            SettingValue::Text("us-east-1".into()),
            "Default S3 region",
        );
    }
    fn initialize(&self, _: &Settings) -> Result<()> {
        Ok(())
    }
}
impl Backend for Smb {
    fn register_settings(&self, s: &mut Settings) {
        s.register(
            "smb:max-credits",
            SettingValue::Int(32),
            "Maximum SMB credits",
        );
    }
    fn initialize(&self, _: &Settings) -> Result<()> {
        Ok(())
    }
}

/// Authoritative daemon state. Clients interact with it through IPC and do not
/// own jobs, connections, or backend configuration.
pub struct Daemon {
    pub settings: Settings,
    pub jobs: JobManager,
    _backends: Vec<Box<dyn Backend + Send + Sync>>,
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
        let backends: Vec<Box<dyn Backend + Send + Sync>> =
            vec![Box::new(Sftp), Box::new(S3), Box::new(Smb)];
        for backend in &backends {
            backend.register_settings(&mut settings);
        }
        for backend in &backends {
            backend.initialize(&settings)?;
        }
        Ok(Self {
            settings,
            jobs: JobManager::new(),
            _backends: backends,
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

    pub fn command(&mut self, line: &str) -> Vec<String> {
        self.jobs.poll();
        let mut words = line.split_whitespace();
        match words.next() {
            Some("help") => vec![
                "Commands:".into(),
                "  connect <url>       connect (demo job)".into(),
                "  get <path>          transfer (demo job)".into(),
                "  mirror <path>       mirror (demo job)".into(),
                "  jobs                list daemon jobs".into(),
                "  cancel <job-id>     cancel a job".into(),
                "  show [setting]      show daemon settings".into(),
                "  set <name> <value>  update a daemon setting".into(),
                "  detach              detach this client".into(),
                "  exit                detach; daemon remains only while jobs run".into(),
            ],
            Some("connect" | "get" | "mirror") => {
                let command = line.split_whitespace().next().unwrap();
                let id = self.jobs.start_demo(line.to_owned(), command != "connect");
                vec![format!("JOB {id}")]
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
