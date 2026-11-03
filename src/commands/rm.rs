/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Exact-path deletion; never replay ambiguous mutations.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{backend::validate_file_path, core::Progress, error::NonRetryableError};
use anyhow::{bail, Result};
const USAGE: &str = "usage: rm [-rf] [--] <paths...>";
pub(super) struct Rm;
impl Command for Rm {
    fn names(&self) -> &'static [&'static str] {
        &["rm"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "rm [-rf] [--] <paths...>",
            "delete remote paths; -r is recursive, dangerous and cannot be undone",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: true,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        parse(arguments.iter().map(String::as_str)).is_ok()
            && (!path.starts_with('-') || arguments.iter().any(|word| word == "--"))
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Args {
    recursive: bool,
    force: bool,
    paths: Vec<String>,
}
fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Args> {
    let mut args = Args {
        recursive: false,
        force: false,
        paths: Vec::new(),
    };
    let mut options = true;
    for word in words {
        match word {
            "--" if options => options = false,
            flag if options && flag.starts_with('-') => {
                if flag == "-" {
                    bail!("unknown rm option -; {USAGE}");
                }
                for option in flag[1..].chars() {
                    match option {
                        'r' => args.recursive = true,
                        'f' => args.force = true,
                        _ => bail!("unknown rm option -{option}; {USAGE}"),
                    }
                }
            }
            path => args.paths.push(path.to_owned()),
        }
    }
    for path in &args.paths {
        // Permit a directory's trailing slash only with -r, but never allow
        // root, dot components, empty names or control characters.
        let path = if args.recursive {
            path.trim_end_matches('/')
        } else {
            path.as_str()
        };
        validate_file_path(path)?;
    }
    Ok(args)
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let args = parse(parser.words())?;
    if args.paths.is_empty() {
        bail!("{USAGE}");
    }
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let id = app
        .jobs
        .start_task(parser.line.into(), move |mut output, progress| async move {
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            let mut failures = 0usize;
            for input in args.paths {
                let result: Result<()> = async {
                    let directory_hint = args.recursive && input.ends_with('/');
                    let input = if args.recursive {
                        input.trim_end_matches('/')
                    } else {
                        &input
                    };
                    let path = active.resolve_listing_path(input)?;
                    let label = format!("rm {path:?}");
                    let _ = progress.send(Progress::Indeterminate {
                        message: label.clone(),
                    });
                    if args.recursive {
                        remove_recursive(
                            active.as_ref(),
                            &path,
                            directory_hint,
                            args.force,
                            &mut output,
                            &progress,
                        )
                        .await
                    } else {
                        active.remove_file(&path).await
                    }
                    .map_err(|error| {
                        error.context(format!(
                            "{label}: not retried; inspect the remote path before repeating"
                        ))
                    })
                }
                .await;
                if let Err(error) = result {
                    if !args.force {
                        return Err(error);
                    }
                    failures += 1;
                    output.write_line(&format!("{error:#}"))?;
                }
            }
            if failures > 0 {
                bail!("rm: {failures} path(s) failed");
            }
            Ok(String::new())
        });
    Ok(CommandResult::JobStarted(id))
}

// Mutations are deliberately sequential and never retried. Re-check every
// directory with lstat semantics before descending, and remove directories only
// after their children. Backends opt in explicitly to these safety guarantees.
async fn remove_recursive(
    connection: &dyn crate::backend::Connection,
    root: &str,
    directory_hint: bool,
    force: bool,
    output: &mut crate::core::OutputStore,
    progress: &tokio::sync::watch::Sender<Progress>,
) -> Result<()> {
    use crate::{listing::Content, walker::Selection};
    enum Work {
        Select(String, bool),
        Directory(String),
    }
    validate_file_path(root)?;
    let mut pending = vec![Work::Select(root.to_owned(), directory_hint)];
    let mut failures = 0usize;
    while let Some(work) = pending.pop() {
        let path = match &work {
            Work::Select(path, _) | Work::Directory(path) => path.clone(),
        };
        let _ = progress.send(Progress::Indeterminate {
            message: format!("rm {path:?}"),
        });
        let result: Result<()> = async {
            match work {
                Work::Directory(path) => connection.remove_empty_directory(&path).await?,
                Work::Select(path, directory_hint) => {
                    match connection.select_for_removal(&path, directory_hint).await? {
                        Selection::File(_) => connection.remove_file(&path).await?,
                        Selection::Directory(entries) => {
                            let mut children = Vec::new();
                            for entry in entries {
                                let (name, directory) = match entry {
                                    Content::File(file) => (file.name, false),
                                    Content::Directory(dir) => {
                                        (dir.name.trim_end_matches('/').to_owned(), true)
                                    }
                                    Content::Comment(_) => continue,
                                    Content::Bucket(_) => {
                                        bail!("refusing to recursively remove a bucket or share")
                                    }
                                };
                                if name.is_empty()
                                    || matches!(name.as_str(), "." | "..")
                                    || name.contains('/')
                                    || name.chars().any(char::is_control)
                                {
                                    bail!("directory listing contains an unsafe child name");
                                }
                                let child = format!("{}/{name}", path.trim_end_matches('/'));
                                children.push(Work::Select(child, directory));
                            }
                            pending.push(Work::Directory(path));
                            pending.extend(children.into_iter().rev());
                        }
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let message = format!("rm {path:?}: {error:#}; not retried");
            if !force {
                bail!("{message}");
            }
            output.write_line(&message)?;
            failures += 1;
        }
        tokio::task::yield_now().await;
    }
    if failures > 0 {
        bail!("recursive rm encountered {failures} error(s)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Connection, core::JobState, daemon::Daemon};
    use std::sync::{Arc, Mutex};
    struct Fake(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl Connection for Fake {
        fn protocol(&self) -> &'static str {
            "fake"
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            Ok(format!("/cwd/{path}"))
        }
        async fn remove_file(&self, path: &str) -> Result<()> {
            self.0.lock().unwrap().push(path.to_owned());
            if path.ends_with("fail") {
                bail!("lost response");
            }
            Ok(())
        }
    }
    #[test]
    fn validates_syntax_and_completion_with_multiple_operands() {
        for words in [
            vec!["-x", "x"],
            vec![""],
            vec!["."],
            vec![".."],
            vec!["x/"],
            vec!["-r", "/"],
        ] {
            assert!(parse(words).is_err());
        }
        assert_eq!(
            parse(["-rf", "--", "-a", "two words/"]).unwrap(),
            Args {
                recursive: true,
                force: true,
                paths: vec!["-a".into(), "two words/".into()]
            }
        );
        assert_eq!(crate::commands::completion_for("rm"), Rm.completion());
        assert!(Rm.accepts_path_completion(&["-rf".into(), "first".into()], "sec"));
        assert!(!Rm.accepts_path_completion(&[], "-r"));
        assert!(Rm.accepts_path_completion(&["--".into()], "-file"));
        let mut app = Daemon::bootstrap().unwrap();
        for line in ["rm", "rm -x x", "rm ''", "rm /"] {
            assert!(app.command(line)[0].contains("rm"));
            assert!(app.jobs.jobs.is_empty());
        }
    }
    struct Tree(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl Connection for Tree {
        fn protocol(&self) -> &'static str {
            "tree"
        }
        async fn select_for_removal(
            &self,
            path: &str,
            _: bool,
        ) -> Result<crate::walker::Selection> {
            use crate::{
                listing::{Content, FileInfo},
                walker::Selection,
            };
            if path == "/root" {
                let file = |name: &str| {
                    Content::File(FileInfo {
                        name: name.into(),
                        size: None,
                        modified: None,
                        unix: None,
                    })
                };
                return Ok(Selection::Directory(vec![file("bad"), file("good")]));
            }
            if path.ends_with("bad") {
                bail!("cannot list");
            }
            Ok(Selection::File(Content::File(FileInfo {
                name: "good".into(),
                size: None,
                modified: None,
                unix: None,
            })))
        }
        async fn remove_file(&self, path: &str) -> Result<()> {
            self.0.lock().unwrap().push(format!("file {path}"));
            Ok(())
        }
        async fn remove_empty_directory(&self, path: &str) -> Result<()> {
            self.0.lock().unwrap().push(format!("dir {path}"));
            Ok(())
        }
    }
    #[tokio::test]
    async fn recursive_force_continues_after_child_errors() {
        for force in [false, true] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let tree = Tree(calls.clone());
            let mut output = crate::core::OutputStore::new(198765);
            let (progress, _) = tokio::sync::watch::channel(Progress::Indeterminate {
                message: String::new(),
            });
            assert!(
                remove_recursive(&tree, "/root", false, force, &mut output, &progress)
                    .await
                    .is_err()
            );
            let calls = calls.lock().unwrap();
            if force {
                assert_eq!(*calls, vec!["file /root/good", "dir /root"]);
            } else {
                assert!(calls.is_empty());
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn errors_stop_or_continue_without_replaying() {
        for force in [false, true] {
            let mut app = Daemon::bootstrap().unwrap();
            let calls = Arc::new(Mutex::new(Vec::new()));
            *app.connection.lock().await = Some(Box::new(Fake(calls.clone())));
            let line = if force {
                "rm -f -- 'first file' fail untouched"
            } else {
                "rm -- 'first file' fail untouched"
            };
            let id = app.command(line)[0]
                .strip_prefix("JOB ")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            for _ in 0..100 {
                tokio::task::yield_now().await;
                if !app.has_running_jobs() {
                    break;
                }
            }
            tokio::time::advance(std::time::Duration::from_secs(120)).await;
            let mut expected = vec!["/cwd/first file", "/cwd/fail"];
            if force {
                expected.push("/cwd/untouched");
            }
            assert_eq!(*calls.lock().unwrap(), expected);
            assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
            assert!(app.jobs.jobs[&id]
                .output
                .read_all()
                .join("\n")
                .contains("not retried"));
        }
    }
}
