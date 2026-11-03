/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Read remote files into private raw job output; the client prints exact bytes
//! without a pager, headings, UTF-8 conversion, or a success footer.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    backend::Connection,
    core::{OutputStore, Progress},
    error::NonRetryableError,
    transfer::{backoff, DownloadSink},
};
use anyhow::{bail, Result};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    time::Duration,
};
use tokio::sync::watch;

const USAGE: &str = "usage: cat [--] <files...>";

pub(super) struct Cat;
impl Command for Cat {
    fn names(&self) -> &'static [&'static str] {
        &["cat"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "cat [--] <files...>",
            "print remote file contents to stdout",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: true,
        }
    }
}
fn files<'a>(words: impl Iterator<Item = &'a str>) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut options = true;
    for word in words {
        if options && word == "--" {
            options = false;
        } else if options && word.starts_with('-') {
            bail!("unknown cat option {word}; {USAGE}");
        } else {
            files.push(word.to_owned());
        }
    }
    if files.is_empty() {
        bail!("{USAGE}");
    }
    Ok(files)
}
fn permanent(error: impl std::fmt::Display) -> anyhow::Error {
    NonRetryableError(error.to_string()).into()
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let files = files(parser.words())?;
    let output = OutputStore::temporary()?;
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let id = app.jobs.start_raw_task(
        parser.line.into(),
        output,
        move |output, progress| async move {
            let mut file = OpenOptions::new()
                .write(true)
                .open(output.path())
                .map_err(permanent)?;
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| permanent("not connected; use connect first"))?;
            // Resolve all sources once, before any reads/retries can change sessions.
            let paths = files
                .iter()
                .map(|path| active.resolve_listing_path(path))
                .collect::<Result<Vec<_>>>()?;
            for path in paths {
                read_file(active.as_ref(), &path, &mut file, &progress).await?;
            }
            Ok(String::new())
        },
    );
    Ok(CommandResult::JobStarted(id))
}

struct Sink<'a> {
    file: &'a mut File,
    offset: &'a mut u64,
    remaining: u64,
}
impl DownloadSink for Sink<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 > self.remaining {
            return Err(permanent("backend sent more data than expected"));
        }
        self.file.write_all(bytes).map_err(permanent)?;
        *self.offset += bytes.len() as u64;
        self.remaining -= bytes.len() as u64;
        Ok(())
    }
}
async fn read_file(
    connection: &dyn Connection,
    path: &str,
    file: &mut File,
    progress: &watch::Sender<Progress>,
) -> Result<()> {
    let label = format!("{path:?}");
    let mut delay = Duration::from_secs(3);
    let mut attempt = 1;
    let metadata = loop {
        let _ = progress.send(Progress::Indeterminate {
            message: format!("reading {label}"),
        });
        match connection.stat_file(path).await {
            Ok(metadata) => break metadata,
            Err(error) => {
                backoff(error, &label, attempt, delay, progress).await?;
                delay = (delay * 2).min(Duration::from_secs(30));
                attempt = attempt.saturating_add(1);
            }
        }
    };
    let start = file.stream_position().map_err(permanent)?;
    let mut offset = 0;
    let mut delay = Duration::from_secs(3);
    let mut attempt = 1;
    loop {
        let before = offset;
        let _ = progress.send(Progress::Indeterminate {
            message: format!("reading {label}"),
        });
        let result = connection
            .download(
                path,
                before,
                &metadata,
                &mut Sink {
                    file,
                    offset: &mut offset,
                    remaining: metadata.size - before,
                },
            )
            .await;
        match result {
            Ok(()) if offset == metadata.size => return Ok(()),
            Ok(()) => return Err(permanent("backend returned incomplete file contents")),
            Err(error) => {
                // A final metadata/checksum/close failure must not be mistaken
                // for success merely because the expected byte count arrived.
                if offset == metadata.size {
                    let end = start
                        .checked_add(before)
                        .ok_or_else(|| permanent("file size overflow"))?;
                    file.set_len(end).map_err(permanent)?;
                    file.seek(SeekFrom::Start(end)).map_err(permanent)?;
                    offset = before;
                }
                backoff(error, &label, attempt, delay, progress).await?;
                delay = (delay * 2).min(Duration::from_secs(30));
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::JobState, daemon::Daemon, transfer::RemoteFile};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    struct Source {
        data: Vec<u8>,
        calls: Arc<Mutex<Vec<(String, u64)>>>,
        fail_once: AtomicBool,
        full_failure: bool,
        stall: bool,
    }
    #[async_trait::async_trait]
    impl Connection for Source {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            Ok(if path.starts_with('/') {
                path.into()
            } else {
                format!("/remote/{path}")
            })
        }
        async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
            if path.ends_with("missing") {
                return Err(permanent("no such file"));
            }
            Ok(RemoteFile {
                size: self.data.len() as u64,
                revision: Some("v1".into()),
            })
        }
        async fn download(
            &self,
            path: &str,
            offset: u64,
            expected: &RemoteFile,
            sink: &mut dyn DownloadSink,
        ) -> Result<()> {
            assert_eq!(expected.revision.as_deref(), Some("v1"));
            self.calls.lock().unwrap().push((path.into(), offset));
            if self.fail_once.swap(false, Ordering::SeqCst) || self.stall {
                let end = if self.full_failure {
                    self.data.len()
                } else {
                    3.min(self.data.len())
                };
                sink.write(&self.data[offset as usize..end])?;
                if self.stall {
                    return std::future::pending().await;
                }
                bail!("transport reset");
            }
            sink.write(&self.data[offset as usize..])
        }
    }
    async fn fixture(
        data: &[u8],
        fail_once: bool,
        full_failure: bool,
        stall: bool,
    ) -> (Daemon, Arc<Mutex<Vec<(String, u64)>>>) {
        let app = Daemon::bootstrap().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        *app.connection.lock().await = Some(Box::new(Source {
            data: data.into(),
            calls: calls.clone(),
            fail_once: AtomicBool::new(fail_once),
            full_failure,
            stall,
        }));
        (app, calls)
    }
    fn start(app: &mut Daemon, command: &str) -> u64 {
        app.command(command)[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse()
            .unwrap()
    }
    async fn finish(app: &mut Daemon) {
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if !app.has_running_jobs() {
                return;
            }
        }
        panic!("cat did not finish");
    }
    fn acknowledge(app: &mut Daemon, id: u64) {
        let parent = app.jobs.jobs[&id]
            .output
            .path()
            .parent()
            .unwrap()
            .to_owned();
        assert_eq!(
            app.command(&format!("ack-output {id}")),
            vec!["output acknowledged"]
        );
        assert!(!parent.exists(), "private cat data was not removed");
    }

    #[test]
    fn arguments_require_remote_files_and_support_end_of_options() {
        assert!(files([].into_iter()).is_err());
        assert!(files(["-n", "a"].into_iter()).is_err());
        assert_eq!(
            files(["--", "-literal", "photo album"].into_iter()).unwrap(),
            vec!["-literal", "photo album"]
        );
    }

    #[tokio::test]
    async fn concatenates_binary_data_without_headers_or_added_newlines_and_keeps_spool_private() {
        let data = b"hello\0\xff\r\nno trailing newline";
        let (mut app, calls) = fixture(data, false, false, false).await;
        let id = start(&mut app, "cat 'one/file name' two/file");
        finish(&mut app).await;
        let job = &app.jobs.jobs[&id];
        assert_eq!(job.state, JobState::Completed);
        assert!(job.raw_output);
        assert_eq!(std::fs::read(job.output.path()).unwrap(), data.repeat(2));
        assert_eq!(
            std::fs::metadata(job.output.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(job.output.path().parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("/remote/one/file name".into(), 0),
                ("/remote/two/file".into(), 0)
            ]
        );
        assert!(app.command(&format!("status {id}"))[0].contains("Completed RAW"));
        acknowledge(&mut app, id);
    }

    #[tokio::test(start_paused = true)]
    async fn interrupted_reads_resume_and_final_byte_failures_are_retried_not_marked_successful() {
        for full_failure in [false, true] {
            let (mut app, calls) = fixture(b"abcdef", true, full_failure, false).await;
            let id = start(&mut app, "cat file");
            tokio::task::yield_now().await;
            assert_eq!(calls.lock().unwrap().len(), 1);
            tokio::time::advance(Duration::from_secs(3)).await;
            finish(&mut app).await;
            assert_eq!(app.jobs.jobs[&id].state, JobState::Completed);
            assert_eq!(
                std::fs::read(app.jobs.jobs[&id].output.path()).unwrap(),
                b"abcdef"
            );
            assert_eq!(
                calls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(_, offset)| *offset)
                    .collect::<Vec<_>>(),
                vec![0, if full_failure { 0 } else { 3 }]
            );
            acknowledge(&mut app, id);
        }
    }

    #[tokio::test]
    async fn empty_files_have_no_footer_and_failures_do_not_mix_diagnostics_into_data() {
        let (mut app, _) = fixture(b"", false, false, false).await;
        let id = start(&mut app, "cat empty");
        finish(&mut app).await;
        assert!(std::fs::read(app.jobs.jobs[&id].output.path())
            .unwrap()
            .is_empty());
        acknowledge(&mut app, id);
        let (mut app, _) = fixture(b"prefix", false, false, false).await;
        let id = start(&mut app, "cat file missing");
        finish(&mut app).await;
        assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
        assert_eq!(
            std::fs::read(app.jobs.jobs[&id].output.path()).unwrap(),
            b"prefix"
        );
        assert!(app.command(&format!("status {id}"))[0].contains("no such file"));
        acknowledge(&mut app, id);
    }

    #[tokio::test]
    async fn incomplete_and_oversized_backend_reads_fail_without_retrying() {
        struct WrongLength {
            size: u64,
            data: &'static [u8],
        }
        #[async_trait::async_trait]
        impl Connection for WrongLength {
            fn protocol(&self) -> &'static str {
                "test"
            }
            async fn stat_file(&self, _: &str) -> Result<RemoteFile> {
                Ok(RemoteFile {
                    size: self.size,
                    revision: None,
                })
            }
            async fn download(
                &self,
                _: &str,
                _: u64,
                _: &RemoteFile,
                sink: &mut dyn DownloadSink,
            ) -> Result<()> {
                sink.write(self.data)
            }
        }
        for (size, data, expected) in [
            (2, b"abc".as_slice(), b"".as_slice()),
            (4, b"ab".as_slice(), b"ab".as_slice()),
        ] {
            let mut app = Daemon::bootstrap().unwrap();
            *app.connection.lock().await = Some(Box::new(WrongLength { size, data }));
            let id = start(&mut app, "cat file");
            finish(&mut app).await;
            assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
            assert_eq!(
                std::fs::read(app.jobs.jobs[&id].output.path()).unwrap(),
                expected
            );
            acknowledge(&mut app, id);
        }
    }

    #[tokio::test]
    async fn cancellation_keeps_partial_bytes_separate_from_the_error_and_releases_the_connection()
    {
        let (mut app, _) = fixture(b"abcdef", false, false, true).await;
        let id = start(&mut app, "cat file");
        tokio::task::yield_now().await;
        app.jobs.cancel(id);
        finish(&mut app).await;
        assert_eq!(app.jobs.jobs[&id].state, JobState::Cancelled);
        assert_eq!(
            std::fs::read(app.jobs.jobs[&id].output.path()).unwrap(),
            b"abc"
        );
        assert!(app.connection.try_lock().is_ok());
        assert!(app.command(&format!("status {id}"))[0].contains("operation cancelled"));
        acknowledge(&mut app, id);
    }
}
