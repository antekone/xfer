/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Recursive remote-directory download into the daemon local directory.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    error::NonRetryableError,
    transfer::{self, GetArgs},
    walker::{self, RemoteSource, WalkControl, WalkEntry, WalkEvent, WalkVisitor},
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

const USAGE: &str = "usage: mirror [-c] [--] <directories...>";

pub(super) struct Mirror;
impl Command for Mirror {
    fn names(&self) -> &'static [&'static str] {
        &["mirror"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "mirror [-c] [--] <directories...>",
            "recursively download remote directories (existing files are skipped)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemoteDirectory,
            multiple_arguments: true,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        Args::parse(
            arguments
                .iter()
                .map(String::as_str)
                .chain(["completion-placeholder"]),
        )
        .is_ok()
            && (!path.starts_with('-') || arguments.iter().any(|arg| arg == "--"))
    }
}

#[derive(Clone)]
struct Args {
    resume: bool,
    directories: Vec<String>,
}
impl Args {
    fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut resume = false;
        let mut directories = Vec::new();
        let mut options = true;
        for word in words {
            match word {
                "-c" if options => resume = true,
                "--" if options => options = false,
                flag if options && flag.starts_with('-') => {
                    bail!("unknown mirror option {flag}; {USAGE}")
                }
                "" => bail!("mirror requires a non-empty directory path; {USAGE}"),
                directory => directories.push(directory.into()),
            }
        }
        if directories.is_empty() {
            bail!(USAGE);
        }
        Ok(Self {
            resume,
            directories,
        })
    }
}

#[derive(Default)]
struct Plan {
    directories: Vec<PathBuf>,
    files: Vec<(String, PathBuf)>,
    saw_root_directory: bool,
}
struct Planner {
    local_root: PathBuf,
    plan: Plan,
}
#[async_trait]
impl WalkVisitor for Planner {
    async fn visit(&mut self, event: WalkEvent, entry: &WalkEntry) -> Result<WalkControl> {
        let target = if entry.relative.is_empty() {
            self.local_root.clone()
        } else {
            // walker validates each child segment, so this cannot escape local_root.
            self.local_root.join(&entry.relative)
        };
        match event {
            WalkEvent::EnterDirectory => {
                if entry.relative.is_empty() {
                    self.plan.saw_root_directory = true;
                }
                self.plan.directories.push(target);
            }
            WalkEvent::File => {
                if entry.relative.is_empty() {
                    bail!("mirror requires directories, not files: {}", entry.path);
                }
                self.plan.files.push((entry.path.clone(), target));
            }
            WalkEvent::LeaveDirectory => {}
        }
        Ok(WalkControl::Continue)
    }
}

fn root_name(path: &str) -> Result<&str> {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .ok_or_else(|| {
            NonRetryableError("mirror needs a named directory, not remote root".into()).into()
        })
}
fn ensure_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            Ok(())
        }
        Ok(_) => bail!("mirror destination {} is not a directory", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)
            .map_err(|error| anyhow::anyhow!("create directory {}: {error}", path.display())),
        Err(error) => Err(anyhow::anyhow!(
            "inspect directory {}: {error}",
            path.display()
        )),
    }
}
fn existing_regular_file(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            Ok(true)
        }
        Ok(_) => bail!(
            "mirror destination {} is not a regular file",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(anyhow::anyhow!("inspect file {}: {error}", path.display())),
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let args = Args::parse(parser.words())?;
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let local_directory = app.local_directory.clone();
    let clobber = app.settings.bool("xfer:clobber");
    let id = app
        .jobs
        .start_task(parser.line.into(), move |mut output, progress| async move {
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            let mut plan = Plan::default();
            for input in &args.directories {
                let remote = active.resolve_directory(Some(input))?;
                let mut planner = Planner {
                    local_root: if input == "." {
                        local_directory.clone()
                    } else {
                        local_directory.join(root_name(&remote)?)
                    },
                    plan: Plan::default(),
                };
                walker::walk(&RemoteSource(active.as_ref()), &remote, &mut planner).await?;
                if !planner.plan.saw_root_directory {
                    bail!("mirror requires directories, not files: {input}");
                }
                plan.directories.extend(planner.plan.directories);
                plan.files.extend(planner.plan.files);
            }
            let mut paths = HashSet::new();
            for path in plan
                .directories
                .iter()
                .chain(plan.files.iter().map(|(_, path)| path))
            {
                if !paths.insert(path.clone()) {
                    bail!(
                        "mirror sources map to the same local path: {}",
                        path.display()
                    );
                }
            }
            for directory in &plan.directories {
                ensure_directory(directory)?;
            }
            for (remote, target) in plan.files {
                // A normal repeated mirror is incremental; -c resumes and clobber replaces.
                if !args.resume && !clobber && existing_regular_file(&target)? {
                    output.write_line(&format!(
                        "skipped existing {}",
                        transfer::safe_name(&target.display().to_string())
                    ))?;
                    continue;
                }
                let parent = target
                    .parent()
                    .expect("file target always has mirror root")
                    .to_owned();
                transfer::get(
                    active.as_ref(),
                    GetArgs {
                        resume: args.resume,
                        files: vec![remote],
                    },
                    parent,
                    clobber,
                    progress.clone(),
                    output.clone(),
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
    #[tokio::test]
    async fn dot_downloads_current_remote_contents_into_current_local_directory() {
        use crate::{
            backend::Connection,
            core::JobState,
            daemon::Daemon,
            listing::{Content, DirectoryInfo, FileInfo, ListingSink},
            transfer::{DownloadSink, RemoteFile},
        };
        use std::time::Duration;

        struct TestConnection;
        #[async_trait]
        impl Connection for TestConnection {
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
            async fn list(&self, path: &str, sink: &mut dyn ListingSink) -> Result<u64> {
                let path = path.trim_end_matches('/');
                if path == "/current" {
                    sink.emit(Content::Directory(DirectoryInfo {
                        name: "nested".into(),
                        modified: None,
                        size: None,
                        unix: None,
                    }))?;
                    sink.emit(Content::File(FileInfo {
                        name: "root.txt".into(),
                        modified: None,
                        size: Some(4),
                        unix: None,
                    }))?;
                } else if path == "/current/nested" {
                    sink.emit(Content::File(FileInfo {
                        name: "file.txt".into(),
                        modified: None,
                        size: Some(4),
                        unix: None,
                    }))?;
                } else {
                    bail!("unexpected path: {path}");
                }
                Ok(0)
            }
            async fn stat_file(&self, _: &str) -> Result<RemoteFile> {
                Ok(RemoteFile {
                    size: 4,
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
                sink.write(b"data")
            }
        }

        let local = tempfile::tempdir().unwrap();
        let mut app = Daemon::bootstrap().unwrap();
        app.local_directory = local.path().to_owned();
        *app.connection.lock().await = Some(Box::new(TestConnection));
        let reply = app.command("mirror .");
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
        assert_eq!(fs::read(local.path().join("root.txt")).unwrap(), b"data");
        assert_eq!(
            fs::read(local.path().join("nested/file.txt")).unwrap(),
            b"data"
        );
        assert!(!local.path().join("current").exists());
    }

    #[test]
    fn arguments_and_completion_support_multiple_directories() {
        let parsed = Args::parse(["-c", "one", "--", "-two"]).unwrap();
        assert!(parsed.resume);
        assert_eq!(parsed.directories, ["one", "-two"]);
        assert!(Args::parse(std::iter::empty()).is_err());
        assert_eq!(
            Mirror.completion(),
            Completion::RemotePath {
                kind: crate::ipc::pb::CompletionKind::RemoteDirectory,
                multiple_arguments: true,
            }
        );
    }
}
