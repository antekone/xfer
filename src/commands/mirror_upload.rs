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
        args(arguments.iter().map(String::as_str)).is_ok()
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
                    let name = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .filter(|name| !name.is_empty())
                        .ok_or_else(|| {
                            NonRetryableError("mirror-upload requires a named directory".into())
                        })?
                        .to_owned();
                    Ok::<_, anyhow::Error>((path, name))
                })
                .collect::<Result<Vec<(PathBuf, String)>>>()?;
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            for (local, name) in local_roots {
                let remote_root = active.resolve_directory(Some(&name))?;
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
