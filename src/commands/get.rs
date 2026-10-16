//! Download to the daemon local directory (-c resumes).
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::core::Daemon;
use anyhow::Result;

pub(super) struct Get;
impl Command for Get {
    fn names(&self) -> &'static [&'static str] {
        &["get"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "get [-c] <files...>",
            "download to the daemon local directory (-c resumes)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            request: "COMPLETE_GET",
            multiple_arguments: true,
        }
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let words = parser.words();
    let args = match crate::transfer::GetArgs::parse(words) {
        Ok(args) => args,
        Err(error) => return vec![error.to_string()],
    };
    let connection = app.connection.clone();
    let directory = app.local_directory.clone();
    let clobber = app.settings.bool("xfer:clobber");
    let id = app
        .jobs
        .start_task(parser.line.to_owned(), move |output, progress| async move {
            let active = connection.lock().await;
            let active = active.as_ref().ok_or_else(|| {
                crate::error::NonRetryableError("not connected; use connect first".into())
            })?;
            crate::transfer::get(active.as_ref(), args, directory, clobber, progress, output).await
        });
    vec![format!("JOB {id}")]
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser).into())
}
