/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Recursive local-directory upload into the current remote directory.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    backend::Connection,
    core::{OutputStore, Progress},
    error::NonRetryableError,
    transfer::{backoff, safe_name},
    walker::{self, LocalSource, WalkControl, WalkEntry, WalkEvent, WalkVisitor},
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const USAGE: &str = "usage: mirror-upload [--] <directories...>";

pub(super) struct MirrorUpload;
impl Command for MirrorUpload {
    fn names(&self) -> &'static [&'static str] {
        &["mirror-upload", "mirror-up"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "mirror-upload [--] <directories...>",
            "recursively upload local directories (alias: mirror-up)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::LocalPath {
            kind: crate::ipc::pb::CompletionKind::LocalDirectory,
            multiple_arguments: true,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        args(
            arguments
                .iter()
                .map(String::as_str)
                .chain(["completion-placeholder"]),
        )
        .is_ok()
            && (!path.starts_with('-') || arguments.iter().any(|arg| arg == "--"))
    }
}

fn args<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Vec<String>> {
    let mut directories = Vec::new();
    let mut options = true;
    for word in words {
        match word {
            "--" if options => options = false,
            flag if options && flag.starts_with('-') => {
                bail!("unknown mirror-upload option {flag}; {USAGE}")
            }
            "" => bail!("mirror-upload requires a non-empty directory path; {USAGE}"),
            directory => directories.push(directory.into()),
        }
    }
    if directories.is_empty() {
        bail!(USAGE);
    }
    Ok(directories)
}

fn remote_join(root: &str, relative: &str) -> String {
    if relative.is_empty() {
        root.into()
    } else {
        format!("{}/{}", root.trim_end_matches('/'), relative)
    }
}

struct Uploader<'a> {
    connection: &'a dyn Connection,
    remote_root: String,
    clobber: bool,
    skip_root: bool,
    output: &'a mut OutputStore,
    progress: tokio::sync::watch::Sender<Progress>,
}
impl Uploader<'_> {
    async fn create(&self, remote: &str) -> Result<()> {
        let mut delay = Duration::from_secs(3);
        let mut attempt = 1;
        loop {
            let _ = self.progress.send(Progress::Indeterminate {
                message: format!("creating {}", safe_name(remote)),
            });
            match self.connection.create_directory(remote, true).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    backoff(error, &safe_name(remote), attempt, delay, &self.progress).await?;
                    delay = (delay * 2).min(Duration::from_secs(30));
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}
#[async_trait]
impl WalkVisitor for Uploader<'_> {
    async fn visit(&mut self, event: WalkEvent, entry: &WalkEntry) -> Result<WalkControl> {
        let remote = remote_join(&self.remote_root, &entry.relative);
        match event {
            WalkEvent::EnterDirectory if self.skip_root && entry.relative.is_empty() => {}
            WalkEvent::EnterDirectory => self.create(&remote).await?,
            WalkEvent::File => {
                let local = Path::new(&entry.path);
                let metadata = fs::symlink_metadata(local).map_err(|error| {
                    NonRetryableError(format!("local file {}: {error}", local.display()))
                })?;
                if !metadata.file_type().is_file() {
                    bail!(
                        "mirror-upload requires regular local files: {}",
                        local.display()
                    );
                }
                let mut delay = Duration::from_secs(3);
                let mut attempt = 1;
                loop {
                    let _ = self.progress.send(Progress::Indeterminate {
                        message: format!("uploading {}", safe_name(&remote)),
                    });
                    match self.connection.upload(local, &remote, self.clobber).await {
                        Ok(()) => break,
                        Err(error) => {
                            backoff(error, &safe_name(&remote), attempt, delay, &self.progress)
                                .await?;
                            delay = (delay * 2).min(Duration::from_secs(30));
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
                self.output.write_line(&format!(
                    "uploaded {} ({} bytes)",
                    safe_name(&remote),
                    metadata.len()
                ))?;
            }
            WalkEvent::LeaveDirectory => {}
        }
        Ok(WalkControl::Continue)
    }
}

// A literal dot means the contents of the current directory, not its basename.
fn upload_directory_name(input: &str, path: &Path) -> Result<Option<String>> {
    if input == "." {
        return Ok(None);
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| NonRetryableError("mirror-upload requires a named directory".into()))?;
    Ok(Some(name.to_owned()))
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let directories = args(parser.words())?;
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let local_directory = app.local_directory.clone();
    let clobber = app.settings.bool("xfer:clobber");
    let id = app
        .jobs
        .start_task(parser.line.into(), move |mut output, progress| async move {
            let local_roots = directories
                .into_iter()
                .map(|input| {
                    let path = crate::local_path::resolve(&local_directory, &input)?;
                    let metadata = fs::symlink_metadata(&path).map_err(|error| {
                        NonRetryableError(format!("local directory {}: {error}", path.display()))
                    })?;
                    if !metadata.file_type().is_dir() {
                        bail!(
                            "mirror-upload requires local directories: {}",
                            path.display()
                        );
                    }
                    let name = upload_directory_name(&input, &path)?;
                    Ok::<_, anyhow::Error>((path, name))
                })
                .collect::<Result<Vec<(PathBuf, Option<String>)>>>()?;
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            for (local, name) in local_roots {
                let remote_root = active.resolve_directory(Some(name.as_deref().unwrap_or(".")))?;
                let local = local.to_str().ok_or_else(|| {
                    NonRetryableError("non-UTF-8 local paths are not supported".into())
                })?;
                walker::walk(
                    &LocalSource,
                    local,
                    &mut Uploader {
                        connection: active.as_ref(),
                        remote_root,
                        clobber,
                        skip_root: name.is_none(),
                        output: &mut output,
                        progress: progress.clone(),
                    },
                )
                .await?;
            }
            Ok(String::new())
        });
    Ok(CommandResult::JobStarted(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dot_uploads_contents_but_named_directories_keep_their_name() {
        assert_eq!(
            upload_directory_name(".", Path::new("/tmp/public/.")).unwrap(),
            None
        );
        assert_eq!(
            upload_directory_name("public", Path::new("/tmp/public")).unwrap(),
            Some("public".into())
        );
    }

    #[tokio::test]
    async fn dot_uploads_into_current_remote_directory_without_creating_it() {
        use crate::{core::JobState, daemon::Daemon};
        use std::sync::{Arc, Mutex};

        struct RecordingConnection(Arc<Mutex<Vec<String>>>);
        #[async_trait]
        impl Connection for RecordingConnection {
            fn protocol(&self) -> &'static str {
                "test"
            }
            fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
                Ok(match path {
                    Some(".") => "/current".into(),
                    Some(name) => format!("/current/{name}"),
                    None => "/initial".into(),
                })
            }
            async fn create_directory(&self, path: &str, _: bool) -> Result<()> {
                self.0.lock().unwrap().push(format!("mkdir {path}"));
                Ok(())
            }
            async fn upload(&self, _: &Path, path: &str, _: bool) -> Result<()> {
                self.0.lock().unwrap().push(format!("upload {path}"));
                Ok(())
            }
        }

        let local = tempfile::tempdir().unwrap();
        fs::write(local.path().join("root.txt"), "root").unwrap();
        fs::create_dir(local.path().join("nested")).unwrap();
        fs::write(local.path().join("nested/file.txt"), "nested").unwrap();
        let operations = Arc::new(Mutex::new(Vec::new()));
        let mut app = Daemon::bootstrap().unwrap();
        app.local_directory = local.path().to_owned();
        *app.connection.lock().await = Some(Box::new(RecordingConnection(operations.clone())));
        let reply = app.command("mirror-upload .");
        let id = reply[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while app.has_running_jobs() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            app.jobs.jobs[&id].state,
            JobState::Completed,
            "{:?}",
            app.jobs.jobs[&id].output.read_all()
        );
        let mut actual = operations.lock().unwrap().clone();
        actual.sort();
        assert_eq!(
            actual,
            [
                "mkdir /current/nested",
                "upload /current/nested/file.txt",
                "upload /current/root.txt"
            ]
        );
        assert!(local.path().join("root.txt").exists());
    }

    #[test]
    fn aliases_and_completion_accept_multiple_directories() {
        assert_eq!(MirrorUpload.names(), ["mirror-upload", "mirror-up"]);
        assert_eq!(args(["one", "--", "-two"]).unwrap(), ["one", "-two"]);
        assert!(args(["-bad"]).is_err());
        assert_eq!(
            MirrorUpload.completion(),
            Completion::LocalPath {
                kind: crate::ipc::pb::CompletionKind::LocalDirectory,
                multiple_arguments: true,
            }
        );
    }
}
