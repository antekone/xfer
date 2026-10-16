//! List buckets, a directory, or a file.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::core::Daemon;
use anyhow::Result;

pub(super) struct Ls;
impl Command for Ls {
    fn names(&self) -> &'static [&'static str] {
        &["ls"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("ls [path]", "list buckets, a directory, or a file"))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            request: "COMPLETE_LS",
            multiple_arguments: false,
        }
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    let path = words.next().unwrap_or("").to_owned();
    if words.next().is_some() {
        return vec!["usage: ls [path]".into()];
    }
    let connection = app.connection.clone();
    let snapshot = std::sync::Arc::new(std::sync::Mutex::new(None));
    let id = app
        .jobs
        .start_retrying_operation(format!("ls {path}"), move |mut output| {
            let connection = connection.clone();
            let path = path.clone();
            let snapshot = snapshot.clone();
            async move {
                let active = connection.lock().await;
                let active = active.as_ref().ok_or_else(|| {
                    crate::error::NonRetryableError("not connected; use connect first".into())
                })?;
                let target = snapshot_listing_path(active.as_ref(), &path, &snapshot)?;
                let count = active.list(&target, &mut output).await?;
                Ok(format!("listed {count} entries"))
            }
        });
    vec![format!("JOB {id}")]
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser).into())
}

pub(crate) fn snapshot_listing_path(
    connection: &dyn crate::backend::Connection,
    path: &str,
    snapshot: &std::sync::Mutex<Option<String>>,
) -> Result<String> {
    let mut saved = snapshot.lock().expect("listing snapshot poisoned");
    if saved.is_none() {
        *saved = Some(connection.resolve_listing_path(path)?);
    }
    Ok(saved.as_ref().unwrap().clone())
}
