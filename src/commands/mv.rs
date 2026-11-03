/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Exact-path file renames. Mutations are never automatically replayed.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{backend::validate_rename_paths, core::Progress, error::NonRetryableError};
use anyhow::{bail, Result};

const USAGE: &str = "usage: mv [-f] [--] <source> <destination>";
pub(super) struct Mv;
impl Command for Mv {
    fn names(&self) -> &'static [&'static str] {
        &["mv"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "mv [-f] [--] <source> <destination>",
            "rename a remote file (-f: allow replacing a file)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: true,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        Args::parse(arguments.iter().map(String::as_str)).is_ok_and(|args| {
            args.paths.len() < 2
                && (!path.starts_with('-') || arguments.iter().any(|arg| arg == "--"))
        })
    }
}
#[derive(Default, Debug, PartialEq, Eq)]
struct Args {
    force: bool,
    paths: Vec<String>,
}
impl Args {
    fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut args = Self::default();
        let mut options = true;
        for word in words {
            match word {
                "--" if options => options = false,
                "-f" | "--force" if options => args.force = true,
                flag if options && flag.starts_with('-') => {
                    bail!("unknown mv option {flag}; {USAGE}")
                }
                path => args.paths.push(path.into()),
            }
        }
        if args.paths.len() > 2 {
            bail!("{USAGE}");
        }
        Ok(args)
    }
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let args = Args::parse(parser.words())?;
    let [source, destination] = args.paths.as_slice() else {
        bail!("{USAGE}");
    };
    validate_rename_paths(source, destination)?;
    let app = context.daemon()?;
    let overwrite = args.force || app.settings.bool("xfer:clobber");
    let connection = app.connection.clone();
    let id = app
        .jobs
        .start_task(parser.line.into(), move |_, progress| async move {
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            let source = active.resolve_listing_path(&args.paths[0])?;
            let destination = active.resolve_listing_path(&args.paths[1])?;
            let label = format!("mv {source:?} -> {destination:?}");
            let _ = progress.send(Progress::Indeterminate {
                message: label.clone(),
            });
            active
                .rename_file(&source, &destination, overwrite)
                .await
                .map_err(|error| {
                    let message = format!(
                        "{label}: {error}; not retried; inspect both paths before repeating"
                    );
                    error.context(message)
                })?;
            Ok(String::new())
        });
    Ok(CommandResult::JobStarted(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Connection, core::JobState, daemon::Daemon};
    use std::sync::{Arc, Mutex};
    type Calls = Arc<Mutex<Vec<(String, String, bool)>>>;
    struct Fake {
        calls: Calls,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl Connection for Fake {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn pwd(&self) -> Result<String> {
            Ok("/original/".into())
        }
        fn resolve_listing_path(&self, path: &str) -> Result<String> {
            Ok(if path.starts_with('/') {
                path.into()
            } else {
                format!("/original/{path}")
            })
        }
        async fn rename_file(&self, a: &str, b: &str, overwrite: bool) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((a.into(), b.into(), overwrite));
            if self.fail {
                bail!("connection reset after rename");
            }
            Ok(())
        }
    }
    #[test]
    fn arity_options_and_exact_paths_are_checked_before_starting_jobs() {
        let mut app = Daemon::bootstrap().unwrap();
        for command in [
            "mv",
            "mv a",
            "mv a b c",
            "mv -x a b",
            "mv '' b",
            "mv a ''",
            "mv a b/",
            "mv . b",
            "mv a ..",
            "mv a /",
            "mv 'a\nb' c",
        ] {
            assert!(app.command(command)[0].contains("mv"), "{command}");
            assert!(app.jobs.jobs.is_empty(), "{command}");
        }
        assert_eq!(
            Args::parse(["--force", "--", "-a", "-b"]).unwrap(),
            Args {
                force: true,
                paths: vec!["-a".into(), "-b".into()]
            }
        );
    }
    #[test]
    fn completes_two_operands_and_uses_the_shared_registry() {
        assert_eq!(crate::commands::completion_for("mv"), Mv.completion());
        for (args, path, expected) in [
            (vec![], "a", true),
            (vec!["source"], "dest", true),
            (vec!["a", "b"], "c", false),
            (vec!["-f", "a"], "b", true),
            (vec!["--"], "-name", true),
            (vec![], "-name", false),
            (vec!["-z"], "a", false),
        ] {
            assert_eq!(
                Mv.accepts_path_completion(
                    &args.into_iter().map(String::from).collect::<Vec<_>>(),
                    path
                ),
                expected
            );
        }
    }
    #[tokio::test(start_paused = true)]
    async fn frozen_clobber_exact_paths_no_cwd_change_and_no_replay_after_lost_reply() {
        for (force, setting, fail) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let mut app = Daemon::bootstrap().unwrap();
            let calls = Calls::default();
            *app.connection.lock().await = Some(Box::new(Fake {
                calls: calls.clone(),
                fail,
            }));
            app.settings
                .set("xfer:clobber", if setting { "true" } else { "false" })
                .unwrap();
            let result = app.command(if force {
                "mv -f -- 'a*.txt' 'b ? 日本語'"
            } else {
                "mv -- 'a*.txt' 'b ? 日本語'"
            });
            let id = result[0]
                .strip_prefix("JOB ")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            app.settings
                .set("xfer:clobber", if setting { "false" } else { "true" })
                .unwrap();
            for _ in 0..100 {
                tokio::task::yield_now().await;
                if !app.has_running_jobs() {
                    break;
                }
            }
            tokio::time::advance(std::time::Duration::from_secs(120)).await;
            assert_eq!(
                *calls.lock().unwrap(),
                vec![(
                    "/original/a*.txt".into(),
                    "/original/b ? 日本語".into(),
                    force || setting
                )]
            );
            assert_eq!(
                app.jobs.jobs[&id].state,
                if fail {
                    JobState::Failed
                } else {
                    JobState::Completed
                }
            );
            if fail {
                assert!(app.jobs.jobs[&id]
                    .output
                    .read_all()
                    .join("\n")
                    .contains("not retried"));
            }
            assert_eq!(
                app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
                "/original/"
            );
        }
    }
    #[tokio::test]
    async fn disconnected_and_unimplemented_moves_fail_without_retries() {
        struct Unsupported;
        #[async_trait::async_trait]
        impl Connection for Unsupported {
            fn protocol(&self) -> &'static str {
                "test"
            }
        }
        for connected in [false, true] {
            let mut app = Daemon::bootstrap().unwrap();
            if connected {
                *app.connection.lock().await = Some(Box::new(Unsupported));
            }
            let result = app.command("mv a b");
            let id = result[0]
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
            assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
            assert!(app.jobs.jobs[&id]
                .output
                .read_all()
                .join("\n")
                .contains(if connected {
                    "mv is not implemented"
                } else {
                    "not connected"
                }));
        }
    }
}
