//! Download planning, local-file safety, resumable retry state and progress.
use crate::{
    backend::Connection,
    core::{OutputStore, Progress},
    error::NonRetryableError,
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Debug)]
pub struct RemoteFile {
    pub size: u64,
    pub revision: Option<String>,
}
pub trait DownloadSink: Send {
    fn write(&mut self, bytes: &[u8]) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferProgress {
    pub file: String,
    pub file_index: usize,
    pub file_count: usize,
    pub done: u64,
    pub total: u64,
    pub transferred: u64,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug)]
pub struct GetArgs {
    pub resume: bool,
    pub files: Vec<String>,
}
impl GetArgs {
    pub fn parse<'a>(words: impl Iterator<Item = &'a str>) -> Result<Self> {
        let mut resume = false;
        let mut files = Vec::new();
        let mut options = true;
        for word in words {
            match word {
                "-c" if options => resume = true,
                "--" if options => options = false,
                flag if options && flag.starts_with('-') => {
                    bail!("unknown get option {flag}; usage: get [-c] [--] <filenames...>")
                }
                file => files.push(file.to_owned()),
            }
        }
        if files.is_empty() {
            bail!("usage: get [-c] [--] <filenames...>");
        }
        Ok(Self { resume, files })
    }
}

fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}
fn local_error(path: &Path, error: impl std::fmt::Display) -> anyhow::Error {
    permanent(format!("local file {}: {error}", path.display()))
}
fn exists_error(path: &Path) -> anyhow::Error {
    permanent(format!(
        "{} already exists; use `set xfer:clobber true` to overwrite, or `get -c` to resume",
        path.display()
    ))
}
fn preflight(path: &Path, resume: bool, clobber: bool) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(permanent(format!(
                    "refusing non-regular local file {} (including symlinks)",
                    path.display()
                )));
            }
            if !resume && !clobber {
                return Err(exists_error(path));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(local_error(path, error)),
    }
    Ok(())
}
fn open_target(path: &Path, resume: bool, clobber: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .mode(0o666);
    if resume || clobber {
        options.create(true);
    } else {
        options.create_new(true);
    }
    let file = options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            exists_error(path)
        } else {
            local_error(path, error)
        }
    })?;
    file.try_lock()
        .map_err(|error| local_error(path, format!("file is busy or cannot be locked: {error}")))?;
    if !file
        .metadata()
        .map_err(|error| local_error(path, error))?
        .is_file()
    {
        return Err(permanent("download target must be a regular file"));
    }
    // Lock BEFORE truncating, and never truncate get -c targets.
    if !resume {
        file.set_len(0).map_err(|error| local_error(path, error))?;
    }
    Ok(file)
}
struct Plan {
    remote: String,
    name: String,
    target: PathBuf,
    metadata: Option<RemoteFile>,
}
fn plans(
    connection: &dyn Connection,
    args: &GetArgs,
    directory: &Path,
    clobber: bool,
) -> Result<Vec<Plan>> {
    let mut names = HashSet::new();
    let mut plans = Vec::new();
    for path in &args.files {
        let remote = connection.resolve_listing_path(path)?;
        if remote.ends_with('/') {
            return Err(permanent(format!(
                "get expects a file, not a bucket/directory: {path}"
            )));
        }
        let name = path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty() && *name != "." && *name != "..")
            .ok_or_else(|| permanent("get requires a filename"))?
            .to_owned();
        if !names.insert(name.clone()) {
            return Err(permanent(format!(
                "multiple sources would download to the same local filename: {name}"
            )));
        }
        let target = directory.join(&name);
        preflight(&target, args.resume, clobber)?;
        plans.push(Plan {
            remote,
            name,
            target,
            metadata: None,
        });
    }
    Ok(plans)
}
pub(crate) async fn backoff(
    error: anyhow::Error,
    label: &str,
    attempt: u64,
    delay: Duration,
    progress: &watch::Sender<Progress>,
) -> Result<()> {
    if error.is::<NonRetryableError>() {
        return Err(error);
    }
    let _ = progress.send(Progress::Indeterminate {
        message: format!(
            "{label} attempt {attempt} failed: {error}; retrying in {}s",
            delay.as_secs()
        ),
    });
    tokio::time::sleep(delay).await;
    Ok(())
}
struct FileSink<'a> {
    file: &'a mut File,
    target: &'a Path,
    progress: &'a watch::Sender<Progress>,
    stats: &'a mut TransferProgress,
    remaining: u64,
}
impl DownloadSink for FileSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 > self.remaining {
            return Err(permanent("backend sent more data than expected"));
        }
        // Bounded stream chunks, not detached filesystem tasks: cancellation
        // cannot leave writes running after the destination lock is released.
        self.file
            .write_all(bytes)
            .map_err(|error| local_error(self.target, error))?;
        self.remaining -= bytes.len() as u64;
        self.stats.done += bytes.len() as u64;
        self.stats.transferred = self.stats.transferred.saturating_add(bytes.len() as u64);
        let _ = self.progress.send(Progress::Transfer(self.stats.clone()));
        Ok(())
    }
}

pub async fn get(
    connection: &dyn Connection,
    args: GetArgs,
    directory: PathBuf,
    clobber: bool,
    progress: watch::Sender<Progress>,
    mut output: OutputStore,
) -> Result<String> {
    let started = Instant::now();
    let mut plans = plans(connection, &args, &directory, clobber)?;
    let mut total = 0u64;
    // Validate all local names before any network or destructive file operation.
    for plan in &mut plans {
        let mut delay = Duration::from_secs(3);
        let mut attempt = 1;
        loop {
            let _ = progress.send(Progress::Indeterminate {
                message: format!("reading metadata: {}", safe_name(&plan.name)),
            });
            match connection.stat_file(&plan.remote).await {
                Ok(metadata) => {
                    total = total
                        .checked_add(metadata.size)
                        .ok_or_else(|| permanent("download size overflow"))?;
                    plan.metadata = Some(metadata);
                    break;
                }
                Err(error) => {
                    backoff(error, &safe_name(&plan.name), attempt, delay, &progress).await?;
                    delay = (delay * 2).min(Duration::from_secs(30));
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
    let file_count = plans.len();
    let mut stats = TransferProgress {
        file: String::new(),
        file_index: 0,
        file_count,
        done: 0,
        total,
        transferred: 0,
        elapsed_ms: 0,
    };
    let mut completed_bytes = 0;
    for (index, plan) in plans.iter().enumerate() {
        let metadata = plan.metadata.as_ref().unwrap();
        // Recheck at open time: create_new prevents races in non-clobber mode.
        let mut file = open_target(&plan.target, args.resume, clobber)?;
        let mut delay = Duration::from_secs(3);
        let mut attempt = 1;
        stats.file = plan.name.clone();
        stats.file_index = index + 1;
        loop {
            let offset = file
                .metadata()
                .map_err(|error| local_error(&plan.target, error))?
                .len();
            if offset > metadata.size {
                return Err(permanent(format!(
                    "{} is larger than the remote file; cannot resume",
                    plan.target.display()
                )));
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| local_error(&plan.target, error))?;
            stats.done = completed_bytes + offset;
            stats.elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
            let _ = progress.send(Progress::Transfer(stats.clone()));
            if offset == metadata.size {
                break;
            }
            let result = connection
                .download(
                    &plan.remote,
                    offset,
                    metadata,
                    &mut FileSink {
                        file: &mut file,
                        target: &plan.target,
                        progress: &progress,
                        stats: &mut stats,
                        remaining: metadata.size - offset,
                    },
                )
                .await;
            match result {
                Ok(()) => {
                    if file
                        .metadata()
                        .map_err(|error| local_error(&plan.target, error))?
                        .len()
                        != metadata.size
                    {
                        return Err(permanent("backend returned an incomplete download"));
                    }
                    break;
                }
                Err(error) => {
                    // An error after the expected final byte may be a checksum
                    // failure: do NOT mark it complete just because length matches.
                    if file
                        .metadata()
                        .map_err(|error| local_error(&plan.target, error))?
                        .len()
                        == metadata.size
                    {
                        file.set_len(offset)
                            .map_err(|error| local_error(&plan.target, error))?;
                    }
                    backoff(error, &safe_name(&plan.name), attempt, delay, &progress).await?;
                    delay = (delay * 2).min(Duration::from_secs(30));
                    attempt = attempt.saturating_add(1);
                }
            }
        }
        file.sync_data()
            .map_err(|error| local_error(&plan.target, error))?;
        completed_bytes += metadata.size;
        output.write_line(&format!(
            "downloaded {} ({} bytes)",
            safe_name(&plan.name),
            metadata.size
        ))?;
    }
    Ok(format!(
        "downloaded {file_count} file(s) in {:.1}s",
        started.elapsed().as_secs_f64()
    ))
}

/// Matches a filename using wildmat's `*` (any sequence) and `?` (one
/// character) operators. Other characters are literals.
pub fn wildmat(pattern: &str, name: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let name = name.chars().collect::<Vec<_>>();
    let mut previous = vec![false; name.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; name.len() + 1];
        match token {
            '*' => {
                current[0] = previous[0];
                for index in 1..=name.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            '?' => {
                for index in 1..=name.len() {
                    current[index] = previous[index - 1];
                }
            }
            literal => {
                for index in 1..=name.len() {
                    current[index] = previous[index - 1] && name[index - 1] == literal;
                }
            }
        }
        previous = current;
    }
    previous[name.len()]
}
pub fn has_wildmat(value: &str) -> bool {
    value.contains(['*', '?'])
}

pub fn safe_name(name: &str) -> String {
    name.chars()
        .flat_map(|ch| {
            if ch.is_control() {
                ch.escape_default().collect::<Vec<_>>()
            } else {
                vec![ch]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildmat_supports_star_question_and_unicode() {
        assert!(wildmat("*.txt", "notes.txt"));
        assert!(wildmat("?.txt", "é.txt"));
        assert!(wildmat("a*?", "abcd"));
        assert!(!wildmat("?.txt", "two.txt"));
        assert!(!wildmat("*.txt", "notes.md"));
    }
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    };

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "xfer-get-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    struct Source {
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<u64>>>,
        fail_first: bool,
        fail_at_end: bool,
    }
    #[async_trait]
    impl Connection for Source {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            Ok(format!("/bucket/{path}"))
        }
        async fn stat_file(&self, _path: &str) -> Result<RemoteFile> {
            Ok(RemoteFile {
                size: self.data.len() as u64,
                revision: Some("v1".into()),
            })
        }
        async fn download(
            &self,
            _path: &str,
            offset: u64,
            _expected: &RemoteFile,
            sink: &mut dyn DownloadSink,
        ) -> Result<()> {
            let first = {
                let mut calls = self.requests.lock().unwrap();
                calls.push(offset);
                calls.len() == 1
            };
            if first && self.fail_first {
                let end = if self.fail_at_end {
                    self.data.len()
                } else {
                    (offset as usize + 3).min(self.data.len())
                };
                sink.write(&self.data[offset as usize..end])?;
                bail!("interrupted");
            }
            sink.write(&self.data[offset as usize..])
        }
    }
    fn source() -> Source {
        Source {
            data: b"abcdef".to_vec(),
            requests: Default::default(),
            fail_first: false,
            fail_at_end: false,
        }
    }
    async fn run(
        source: &Source,
        dir: &Temp,
        resume: bool,
        clobber: bool,
        files: &[&str],
    ) -> Result<TransferProgress> {
        let (tx, rx) = watch::channel(Progress::Indeterminate {
            message: "start".into(),
        });
        get(
            source,
            GetArgs {
                resume,
                files: files.iter().map(|name| name.to_string()).collect(),
            },
            dir.0.clone(),
            clobber,
            tx,
            OutputStore::new(1),
        )
        .await?;
        let snapshot = rx.borrow().clone();
        match snapshot {
            Progress::Transfer(stats) => Ok(stats),
            _ => panic!("missing transfer progress"),
        }
    }
    #[test]
    fn parses_continue_and_literal_dash_names() {
        let args = GetArgs::parse(["-c", "a.txt", "--", "-b"].into_iter()).unwrap();
        assert!(args.resume);
        assert_eq!(args.files, vec!["a.txt", "-b"]);
        assert!(GetArgs::parse(["-c"].into_iter()).is_err());
        assert!(GetArgs::parse(["-x", "a"].into_iter()).is_err());
    }
    #[tokio::test]
    async fn downloads_multiple_files_preserving_basenames() {
        let dir = Temp::new();
        let source = source();
        let stats = run(
            &source,
            &dir,
            false,
            false,
            &["folder/a.txt", "photo album.jpg"],
        )
        .await
        .unwrap();
        assert_eq!(fs::read(dir.0.join("a.txt")).unwrap(), b"abcdef");
        assert_eq!(fs::read(dir.0.join("photo album.jpg")).unwrap(), b"abcdef");
        assert_eq!(stats.done, 12);
        assert_eq!(stats.total, 12);
        assert_eq!(stats.transferred, 12);
    }
    #[tokio::test]
    async fn existing_files_require_clobber_and_remain_untouched_on_error() {
        let dir = Temp::new();
        let source = source();
        fs::write(dir.0.join("a.txt"), b"keep me").unwrap();
        let error = run(&source, &dir, false, false, &["a.txt"])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("xfer:clobber true"));
        assert_eq!(fs::read(dir.0.join("a.txt")).unwrap(), b"keep me");
        run(&source, &dir, false, true, &["a.txt"]).await.unwrap();
        assert_eq!(fs::read(dir.0.join("a.txt")).unwrap(), b"abcdef");
    }
    #[tokio::test]
    async fn continue_resumes_missing_partial_and_already_complete_files() {
        let dir = Temp::new();
        let source = source();
        fs::write(dir.0.join("partial"), b"abc").unwrap();
        fs::write(dir.0.join("complete"), b"abcdef").unwrap();
        run(
            &source,
            &dir,
            true,
            false,
            &["partial", "missing", "complete"],
        )
        .await
        .unwrap();
        assert_eq!(*source.requests.lock().unwrap(), vec![3, 0]);
        for name in ["partial", "missing", "complete"] {
            assert_eq!(fs::read(dir.0.join(name)).unwrap(), b"abcdef");
        }
    }
    #[tokio::test]
    async fn continue_rejects_oversized_files_without_truncating() {
        let dir = Temp::new();
        let source = source();
        fs::write(dir.0.join("large"), b"too large").unwrap();
        assert!(run(&source, &dir, true, true, &["large"]).await.is_err());
        assert_eq!(fs::read(dir.0.join("large")).unwrap(), b"too large");
    }
    #[tokio::test(start_paused = true)]
    async fn retries_stream_failure_from_last_written_byte() {
        let dir = Temp::new();
        let mut source = source();
        source.fail_first = true;
        run(&source, &dir, false, false, &["a"]).await.unwrap();
        assert_eq!(*source.requests.lock().unwrap(), vec![0, 3]);
        assert_eq!(fs::read(dir.0.join("a")).unwrap(), b"abcdef");
    }
    #[tokio::test(start_paused = true)]
    async fn full_length_error_does_not_silently_mark_corrupt_attempt_complete() {
        let dir = Temp::new();
        let mut source = source();
        source.fail_first = true;
        source.fail_at_end = true;
        run(&source, &dir, false, false, &["a"]).await.unwrap();
        assert_eq!(*source.requests.lock().unwrap(), vec![0, 0]);
    }
    #[tokio::test]
    async fn empty_files_are_created_without_an_object_body_request() {
        let dir = Temp::new();
        let mut source = source();
        source.data.clear();
        let stats = run(&source, &dir, false, false, &["empty"]).await.unwrap();
        assert_eq!(fs::metadata(dir.0.join("empty")).unwrap().len(), 0);
        assert_eq!(stats.total, 0);
        assert!(source.requests.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn multi_file_conflict_does_not_create_earlier_targets() {
        let dir = Temp::new();
        let source = source();
        fs::write(dir.0.join("existing"), b"safe").unwrap();
        assert!(run(&source, &dir, false, false, &["new", "existing"])
            .await
            .is_err());
        assert!(!dir.0.join("new").exists());
        assert_eq!(fs::read(dir.0.join("existing")).unwrap(), b"safe");
    }

    #[tokio::test]
    async fn duplicate_names_and_symlinks_are_rejected() {
        let dir = Temp::new();
        let source = source();
        assert!(run(&source, &dir, false, true, &["one/a", "two/a"])
            .await
            .is_err());
        fs::write(dir.0.join("real"), b"safe").unwrap();
        std::os::unix::fs::symlink(dir.0.join("real"), dir.0.join("alias")).unwrap();
        assert!(run(&source, &dir, true, true, &["alias"]).await.is_err());
        assert_eq!(fs::read(dir.0.join("real")).unwrap(), b"safe");
    }
    #[test]
    fn target_lock_is_acquired_before_clobber_truncation() {
        let dir = Temp::new();
        let path = dir.0.join("busy");
        fs::write(&path, b"safe").unwrap();
        let _held = open_target(&path, true, false).unwrap();
        assert!(open_target(&path, false, true).is_err());
        assert_eq!(fs::read(path).unwrap(), b"safe");
    }
}
