/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Remote apparent byte usage, implemented as a directory-walker visitor.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    core::OutputStore,
    error::NonRetryableError,
    listing::{human_size, Content},
    transfer::safe_name,
    walker::{self, RemoteSource, WalkControl, WalkEntry, WalkEvent, WalkVisitor},
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

pub(super) struct Du;
impl Command for Du {
    fn names(&self) -> &'static [&'static str] {
        &["du"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "du [-sh] [--] [paths...]",
            "show remote usage (-s: roots only, -h: human-readable)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: true,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Args {
    summary: bool,
    human_readable: bool,
    paths: Vec<String>,
}
fn args<'a>(words: impl Iterator<Item = &'a str>) -> Result<Args> {
    let mut args = Args {
        summary: false,
        human_readable: false,
        paths: Vec::new(),
    };
    let mut options = true;
    for word in words {
        match word {
            "--" if options => options = false,
            flag if options && flag.starts_with('-') => {
                for option in flag[1..].chars() {
                    match option {
                        's' => args.summary = true,
                        'h' => args.human_readable = true,
                        _ => bail!("unknown du option -{option}; usage: du [-sh] [--] [paths...]"),
                    }
                }
                if flag == "-" {
                    bail!("unknown du option -; usage: du [-sh] [--] [paths...]");
                }
            }
            path => args.paths.push(path.into()),
        }
    }
    if args.paths.is_empty() {
        args.paths.push(".".into());
    }
    Ok(args)
}
struct Usage<'a> {
    output: &'a mut OutputStore,
    root_label: &'a str,
    summary: bool,
    human_readable: bool,
    totals: Vec<u64>,
}
impl Usage<'_> {
    fn emit(&mut self, size: u64, entry: &WalkEntry) -> Result<()> {
        let label = if entry.relative.is_empty() {
            self.root_label.to_owned()
        } else {
            format!(
                "{}/{}",
                self.root_label.trim_end_matches('/'),
                entry.relative
            )
        };
        let size = if self.human_readable {
            human_size(size)
        } else {
            size.to_string()
        };
        self.output
            .write_line(&format!("{size:>9}  {}", safe_name(&label)))
    }
    fn add(&mut self, size: u64) -> Result<()> {
        if let Some(total) = self.totals.last_mut() {
            *total = total
                .checked_add(size)
                .ok_or_else(|| NonRetryableError("du size overflow".into()))?;
        }
        Ok(())
    }
}
#[async_trait]
impl WalkVisitor for Usage<'_> {
    async fn visit(&mut self, event: WalkEvent, entry: &WalkEntry) -> Result<WalkControl> {
        match event {
            WalkEvent::EnterDirectory => self.totals.push(0),
            WalkEvent::File => {
                let size = match &entry.content {
                    Content::File(file) => file.size,
                    Content::Directory(dir) => dir.size, // Unfollowed directory symlink.
                    _ => None,
                }
                .ok_or_else(|| {
                    NonRetryableError(format!("size unavailable for {}", safe_name(&entry.path)))
                })?;
                self.add(size)?;
                if entry.relative.is_empty() {
                    self.emit(size, entry)?;
                }
            }
            WalkEvent::LeaveDirectory => {
                let total = self
                    .totals
                    .pop()
                    .expect("walker directory events must be balanced");
                self.add(total)?;
                if !self.summary || entry.relative.is_empty() {
                    self.emit(total, entry)?;
                }
            }
        }
        Ok(WalkControl::Continue)
    }
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let args = args(parser.words())?;
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let snapshot = Arc::new(Mutex::new(None::<Vec<String>>));
    let id = app
        .jobs
        .start_retrying_operation(parser.line.into(), move |mut output| {
            let connection = connection.clone();
            let snapshot = snapshot.clone();
            let args = args.clone();
            async move {
                let active = connection.lock().await;
                let active = active
                    .as_ref()
                    .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
                let paths = {
                    let mut saved = snapshot.lock().expect("du snapshot poisoned");
                    if saved.is_none() {
                        *saved = Some(
                            args.paths
                                .iter()
                                .map(|path| active.resolve_listing_path(path))
                                .collect::<Result<Vec<_>>>()?,
                        );
                    }
                    saved.as_ref().unwrap().clone()
                };
                for (path, label) in paths.iter().zip(&args.paths) {
                    walker::walk(
                        &RemoteSource(active.as_ref()),
                        path,
                        &mut Usage {
                            output: &mut output,
                            root_label: label,
                            summary: args.summary,
                            human_readable: args.human_readable,
                            totals: Vec::new(),
                        },
                    )
                    .await?;
                }
                Ok(String::new())
            }
        });
    Ok(CommandResult::JobStarted(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::Connection,
        daemon::Daemon,
        listing::{DirectoryInfo, FileInfo, ListingSink},
    };

    fn file(name: &str, size: Option<u64>) -> Content {
        Content::File(FileInfo {
            name: name.into(),
            size,
            modified: None,
            unix: None,
        })
    }
    struct Tree;
    #[async_trait]
    impl Connection for Tree {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            Ok(if path == "." {
                "/root/".into()
            } else {
                path.into()
            })
        }
        async fn list(&self, path: &str, sink: &mut dyn ListingSink) -> Result<u64> {
            match path {
                "/root/" => {
                    sink.emit(file("a", Some(5)))?;
                    sink.emit(Content::Directory(DirectoryInfo {
                        name: "sub/".into(),
                        modified: None,
                        size: Some(4096),
                        unix: None,
                    }))?;
                    Ok(2)
                }
                "/root/sub/" => {
                    sink.emit(file("b", Some(7)))?;
                    Ok(1)
                }
                "/file" => {
                    sink.emit_file(file("file", Some(3)))?;
                    Ok(1)
                }
                _ => bail!("unexpected path: {path}"),
            }
        }
    }
    #[test]
    fn arguments_default_to_current_directory_and_support_literal_dashes() {
        assert_eq!(args(std::iter::empty()).unwrap().paths, ["."]);
        assert_eq!(
            args(["-s", "--", "-dir"].into_iter()).unwrap(),
            Args {
                summary: true,
                human_readable: false,
                paths: vec!["-dir".into()],
            }
        );
        assert!(args(["-x"].into_iter()).is_err());
    }
    #[test]
    fn human_sizes_and_combined_options() {
        assert_eq!(human_size(0), "0B");
        assert_eq!(human_size(1023), "1023B");
        assert_eq!(human_size(1024), "1.0KiB");
        assert_eq!(human_size(1536), "1.5KiB");
        assert_eq!(human_size(1024 * 1024), "1.0MiB");
        assert_eq!(human_size(u64::MAX), "16.0EiB");
        let parsed = args(["-sh"].into_iter()).unwrap();
        assert!(parsed.summary && parsed.human_readable);
        assert_eq!(parsed, args(["-s", "-h"].into_iter()).unwrap());
    }

    #[tokio::test]
    async fn human_readable_output() {
        let mut output = OutputStore::temporary().unwrap();
        let entry = WalkEntry {
            path: "/file".into(),
            relative: String::new(),
            content: file("file", Some(1536)),
        };
        Usage {
            output: &mut output,
            root_label: "/file",
            summary: false,
            human_readable: true,
            totals: Vec::new(),
        }
        .visit(WalkEvent::File, &entry)
        .await
        .unwrap();
        assert_eq!(output.read_all(), ["   1.5KiB  /file"]);
    }

    #[tokio::test]
    async fn usage_is_postorder_and_counts_file_bytes_not_directory_attributes() {
        let mut output = OutputStore::temporary().unwrap();
        walker::walk(
            &RemoteSource(&Tree),
            "/root/",
            &mut Usage {
                output: &mut output,
                root_label: ".",
                summary: false,
                human_readable: false,
                totals: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(output.read_all(), ["        7  ./sub", "       12  ."]);
    }
    #[tokio::test]
    async fn summary_and_explicit_file_selection() {
        let mut output = OutputStore::temporary().unwrap();
        walker::walk(
            &RemoteSource(&Tree),
            "/root/",
            &mut Usage {
                output: &mut output,
                root_label: "root",
                summary: true,
                human_readable: false,
                totals: Vec::new(),
            },
        )
        .await
        .unwrap();
        walker::walk(
            &RemoteSource(&Tree),
            "/file",
            &mut Usage {
                output: &mut output,
                root_label: "/file",
                summary: true,
                human_readable: false,
                totals: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(output.read_all(), ["       12  root", "        3  /file"]);
    }
    #[tokio::test]
    async fn unknown_sizes_and_overflow_are_errors_not_silent_underestimates() {
        let mut output = OutputStore::temporary().unwrap();
        let mut usage = Usage {
            output: &mut output,
            root_label: ".",
            summary: false,
            human_readable: false,
            totals: vec![u64::MAX],
        };
        let mut entry = WalkEntry {
            path: "/unknown".into(),
            relative: "unknown".into(),
            content: file("unknown", None),
        };
        assert!(usage
            .visit(WalkEvent::File, &entry)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        entry.content = file("one", Some(1));
        assert!(usage
            .visit(WalkEvent::File, &entry)
            .await
            .unwrap_err()
            .to_string()
            .contains("overflow"));
    }
    #[tokio::test]
    async fn command_runs_as_a_daemon_job() {
        let mut app = Daemon::bootstrap().unwrap();
        *app.connection.lock().await = Some(Box::new(Tree));
        let response = app.command("du -s");
        let id = response[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        for _ in 0..50 {
            tokio::task::yield_now().await;
            app.jobs.poll();
            if !app.has_running_jobs() {
                break;
            }
        }
        assert_eq!(app.jobs.jobs[&id].state, crate::core::JobState::Completed);
        assert_eq!(app.jobs.jobs[&id].output.read_all(), ["       12  ."]);
    }
}
