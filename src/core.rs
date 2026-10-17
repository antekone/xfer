//! Shared job management, settings, progress and output-spooling primitives.

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
        // Job IDs are local to a daemon; separate instances must never overwrite each other's output.
        let path = crate::cache_dir()
            .join("jobs")
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
    Secret(Option<crate::credentials::Password>),
}
impl SettingValue {
    pub fn display(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Text(value) => value.clone(),
            Self::Secret(Some(_)) => "[redacted]".into(),
            Self::Secret(None) => "[not set]".into(),
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
        let source = zeroize::Zeroizing::new(source);
        for (line_no, line) in source.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // Match a registered namespaced key first. Splitting at the last
            // colon would misread passwords containing ':' and could echo part
            // of the secret as an unknown setting name.
            let known = self
                .values
                .keys()
                .filter_map(|name| {
                    line.strip_prefix(name)
                        .and_then(|rest| rest.trim_start().strip_prefix(':'))
                        .map(|value| (name.clone(), value))
                })
                .max_by_key(|(name, _)| name.len());
            let (name, value) = known
                .or_else(|| {
                    line.split_once(": ")
                        .or_else(|| line.rsplit_once(':'))
                        .map(|(name, value)| (name.to_owned(), value))
                })
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
    pub fn secret(&self, name: &str) -> Option<&crate::credentials::Password> {
        match self.values.get(name).map(|setting| &setting.value) {
            Some(SettingValue::Secret(value)) => value.as_ref(),
            _ => None,
        }
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
            SettingValue::Secret(_) => {
                SettingValue::Secret(Some(crate::credentials::Password::new(raw.into())))
            }
        };
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

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
}
