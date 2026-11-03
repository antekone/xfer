/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Create remote directories without changing the connection's working directory.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{core::Progress, error::NonRetryableError, transfer::backoff};
use anyhow::{bail, Result};
use std::time::Duration;

const USAGE: &str = "usage: mkdir [-p] [--] <directories...>";

pub(super) struct Mkdir;
impl Command for Mkdir {
    fn names(&self) -> &'static [&'static str] {
        &["mkdir"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "mkdir [-p] [--] <directories...>",
            "create remote directories (-p: create missing parents)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemoteDirectory,
            multiple_arguments: true,
        }
    }
    fn accepts_path_completion(&self, arguments: &[String], path: &str) -> bool {
        Args::parse(arguments.iter().map(String::as_str)).is_ok()
            && (!path.starts_with('-') || arguments.iter().any(|arg| arg == "--"))
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    parents: bool,
    paths: Vec<String>,
}
impl Args {
    fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut args = Self::default();
        let mut options = true;
        for word in words {
            match word {
                "--" if options => options = false,
                "-p" | "--parents" if options => args.parents = true,
                flag if options && flag.starts_with('-') => {
                    bail!("unknown mkdir option {flag}; {USAGE}")
                }
                "" => bail!("mkdir requires a non-empty directory path; {USAGE}"),
                path => args.paths.push(path.into()),
            }
        }
        Ok(args)
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let args = Args::parse(parser.words())?;
    if args.paths.is_empty() {
        bail!("{USAGE}");
    }
    let app = context.daemon()?;
    let connection = app.connection.clone();
    let id = app
        .jobs
        .start_task(parser.line.into(), move |_, progress| async move {
            let active = connection.lock().await;
            let active = active
                .as_ref()
                .ok_or_else(|| NonRetryableError("not connected; use connect first".into()))?;
            // Freeze all operands before retries; never change remote cwd to walk parents.
            let paths = args
                .paths
                .iter()
                .map(|path| active.resolve_directory(Some(path)))
                .collect::<Result<Vec<_>>>()?;
            for path in paths {
                let label = format!("mkdir {path:?}");
                let mut delay = Duration::from_secs(3);
                let mut attempt = 1;
                loop {
                    let _ = progress.send(Progress::Indeterminate {
                        message: label.clone(),
                    });
                    match active.create_directory(&path, args.parents).await {
                        Ok(()) => break,
                        Err(error) => {
                            // Only -p is replayable: a lost response to plain mkdir
                            // leaves an ambiguous outcome, not a safe retry.
                            if !args.parents {
                                bail!("{label}: {error}");
                            }
                            backoff(error, &label, attempt, delay, &progress)
                                .await
                                .map_err(|error| anyhow::anyhow!("{label}: {error}"))?;
                            delay = (delay * 2).min(Duration::from_secs(30));
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
            }
            Ok(String::new())
        });
    Ok(CommandResult::JobStarted(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Connection, core::JobState, daemon::Daemon};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    type Calls = Arc<Mutex<Vec<(String, bool)>>>;
    struct Fake {
        calls: Calls,
        fail_once: AtomicBool,
        permanent: bool,
    }
    #[async_trait::async_trait]
    impl Connection for Fake {
        fn protocol(&self) -> &'static str {
            "test"
        }
        fn pwd(&self) -> Result<String> {
            Ok("/original/".into())
        }
        fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
            let path = path.unwrap();
            Ok(if path.starts_with('/') {
                path.to_owned()
            } else {
                format!("/original/{path}/")
            })
        }
        async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
            self.calls.lock().unwrap().push((path.into(), parents));
            if self.fail_once.swap(false, Ordering::SeqCst) {
                if self.permanent {
                    return Err(NonRetryableError("permission denied".into()).into());
                }
                bail!("connection reset");
            }
            Ok(())
        }
    }
    async fn fixture(fail: bool, permanent: bool) -> (Daemon, Calls) {
        let app = Daemon::bootstrap().unwrap();
        let calls = Calls::default();
        *app.connection.lock().await = Some(Box::new(Fake {
            calls: calls.clone(),
            fail_once: AtomicBool::new(fail),
            permanent,
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
        panic!("mkdir did not finish");
    }

    #[test]
    fn arguments_validate_flags_and_allow_literal_names_after_double_dash() {
        assert_eq!(
            Args::parse(["-p", "one/two", "three four"]).unwrap(),
            Args {
                parents: true,
                paths: vec!["one/two".into(), "three four".into()]
            }
        );
        assert_eq!(
            Args::parse(["--parents", "--", "-p"]).unwrap(),
            Args {
                parents: true,
                paths: vec!["-p".into()]
            }
        );
        let mut app = Daemon::bootstrap().unwrap();
        for command in ["mkdir", "mkdir -p", "mkdir --", "mkdir ''", "mkdir -x a"] {
            assert!(
                app.command(command)[0].contains("usage: mkdir"),
                "{command}"
            );
        }
        assert!(app.jobs.jobs.is_empty());
    }

    #[test]
    fn completion_supports_flags_multiple_operands_and_only_directories() {
        assert_eq!(
            Mkdir.completion(),
            Completion::RemotePath {
                kind: crate::ipc::pb::CompletionKind::RemoteDirectory,
                multiple_arguments: true
            }
        );
        for (args, path, accepted) in [
            (vec!["-p", "first"], "second", true),
            (vec!["--parents"], "", true),
            (vec!["--"], "-literal", true),
            (vec![], "-p", false),
            (vec!["-x"], "path", false),
        ] {
            assert_eq!(
                Mkdir.accepts_path_completion(
                    &args.into_iter().map(String::from).collect::<Vec<_>>(),
                    path
                ),
                accepted
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn parents_retry_frozen_operands_without_changing_cwd_or_printing_a_footer() {
        let (mut app, calls) = fixture(true, false).await;
        let id = start(&mut app, "mkdir -p 'first dir/child' /absolute/other");
        tokio::task::yield_now().await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(3)).await;
        finish(&mut app).await;
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("/original/first dir/child/".into(), true),
                ("/original/first dir/child/".into(), true),
                ("/absolute/other".into(), true)
            ]
        );
        assert_eq!(app.jobs.jobs[&id].state, JobState::Completed);
        assert!(app.jobs.jobs[&id].output.read_all().is_empty());
        assert_eq!(
            app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
            "/original/"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn plain_mkdir_is_not_replayed_and_permanent_failures_never_retry() {
        for (parents, permanent) in [(false, false), (false, true), (true, true)] {
            let (mut app, calls) = fixture(true, permanent).await;
            let id = start(
                &mut app,
                if parents {
                    "mkdir -p target"
                } else {
                    "mkdir target"
                },
            );
            finish(&mut app).await;
            assert_eq!(calls.lock().unwrap().len(), 1);
            assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
            assert!(app.jobs.jobs[&id]
                .output
                .read_all()
                .join("\n")
                .contains(if permanent {
                    "permission denied"
                } else {
                    "connection reset"
                }));
        }
    }

    #[tokio::test]
    async fn disconnected_and_unsupported_connections_fail_without_retrying() {
        let mut app = Daemon::bootstrap().unwrap();
        let id = start(&mut app, "mkdir -p target");
        finish(&mut app).await;
        assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
        assert!(app.jobs.jobs[&id]
            .output
            .read_all()
            .join("\n")
            .contains("not connected"));
        struct Unsupported;
        #[async_trait::async_trait]
        impl Connection for Unsupported {
            fn protocol(&self) -> &'static str {
                "unsupported"
            }
        }
        *app.connection.lock().await = Some(Box::new(Unsupported));
        let id = start(&mut app, "mkdir -p target");
        finish(&mut app).await;
        assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
        assert!(app.jobs.jobs[&id]
            .output
            .read_all()
            .join("\n")
            .contains("mkdir is not implemented for unsupported"));
    }
}
