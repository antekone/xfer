//! Non-recursive exact-file deletion; never replay ambiguous mutations.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{backend::validate_file_path, core::Progress, error::NonRetryableError};
use anyhow::{bail, Result};
const USAGE: &str = "usage: rm [--] <files...>";
pub(super) struct Rm;
impl Command for Rm {
    fn names(&self) -> &'static [&'static str] {
        &["rm"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("rm [--] <files...>", "delete remote files (non-recursive)"))
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
fn parse<'a>(words: impl IntoIterator<Item = &'a str>) -> Result<Vec<String>> {
    let mut options = true;
    let mut paths = Vec::new();
    for word in words {
        match word {
            "--" if options => options = false,
            flag if options && flag.starts_with('-') => bail!("unknown rm option {flag}; {USAGE}"),
            path => {
                validate_file_path(path)?;
                paths.push(path.to_owned());
            }
        }
    }
    Ok(paths)
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let paths = parse(parser.words())?;
    if paths.is_empty() {
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
            let paths = paths
                .iter()
                .map(|path| active.resolve_listing_path(path))
                .collect::<Result<Vec<_>>>()?;
            for path in paths {
                let label = format!("rm {path:?}");
                let _ = progress.send(Progress::Indeterminate {
                    message: label.clone(),
                });
                active.remove_file(&path).await.map_err(|error| {
                    let message = format!(
                        "{label}: {error}; not retried; inspect the remote path before repeating"
                    );
                    error.context(message)
                })?;
            }
            Ok(String::new())
        });
    Ok(CommandResult::JobStarted(id))
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
        for words in [vec!["-r", "x"], vec![""], vec!["."], vec![".."], vec!["x/"]] {
            assert!(parse(words).is_err());
        }
        assert_eq!(
            parse(["--", "-a", "two words"]).unwrap(),
            vec!["-a", "two words"]
        );
        assert_eq!(crate::commands::completion_for("rm"), Rm.completion());
        assert!(Rm.accepts_path_completion(&["first".into()], "sec"));
        assert!(!Rm.accepts_path_completion(&[], "-r"));
        assert!(Rm.accepts_path_completion(&["--".into()], "-file"));
        let mut app = Daemon::bootstrap().unwrap();
        for line in ["rm", "rm -r x", "rm ''", "rm /"] {
            assert!(app.command(line)[0].contains("rm"));
            assert!(app.jobs.jobs.is_empty());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn multiple_files_stop_at_first_failure_without_replaying() {
        let mut app = Daemon::bootstrap().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        *app.connection.lock().await = Some(Box::new(Fake(calls.clone())));
        let output = app.command("rm -- 'first file' fail untouched");
        let id = output[0]
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
        assert_eq!(*calls.lock().unwrap(), vec!["/cwd/first file", "/cwd/fail"]);
        assert_eq!(app.jobs.jobs[&id].state, JobState::Failed);
        assert!(app.jobs.jobs[&id]
            .output
            .read_all()
            .join("\n")
            .contains("not retried"));
    }
}
