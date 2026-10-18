//! Download to the daemon local directory (-c resumes).
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::{
    backend::Connection,
    daemon::Daemon,
    error::NonRetryableError,
    listing::{Content, ListingSink},
    transfer::{has_wildmat, wildmat, GetArgs},
};
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
            kind: crate::ipc::pb::CompletionKind::RemotePath,
            multiple_arguments: true,
        }
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> CommandResult {
    let words = parser.words();
    let args = match GetArgs::parse(words) {
        Ok(args) => args,
        Err(error) => return vec![error.to_string()].into(),
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
            let args = expand_remote(active.as_ref(), args).await?;
            crate::transfer::get(active.as_ref(), args, directory, clobber, progress, output).await
        });
    CommandResult::JobStarted(id)
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser))
}

struct Files(Vec<String>);
impl ListingSink for Files {
    fn emit(&mut self, content: Content) -> Result<()> {
        if let Content::File(file) = content {
            self.0.push(file.name);
        }
        Ok(())
    }
}

async fn expand_remote(connection: &dyn Connection, args: GetArgs) -> Result<GetArgs> {
    if !args.files.iter().any(|file| has_wildmat(file)) {
        return Ok(args);
    }
    let mut files = Files(Vec::new());
    connection.list(".", &mut files).await?;
    files.0.sort();
    let mut expanded = Vec::new();
    for input in args.files {
        if !has_wildmat(&input) {
            expanded.push(input);
            continue;
        }
        let matches = files
            .0
            .iter()
            .filter(|name| wildmat(&input, name))
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(NonRetryableError(format!("no remote files match {input:?}")).into());
        }
        expanded.extend(matches);
    }
    Ok(GetArgs {
        resume: args.resume,
        files: expanded,
    })
}
