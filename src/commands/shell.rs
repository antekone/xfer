//! Local Bash escapes. Bash receives untouched source and owns the terminal.
use super::{Command, CommandContext, CommandResult, ExecutionSite, Handler, ParserContext};
use anyhow::{bail, Context, Result};

pub(super) struct Shell;
impl Command for Shell {
    fn names(&self) -> &'static [&'static str] {
        &["!"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn site(&self) -> ExecutionSite {
        ExecutionSite::Client
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("!<command>", "run local Bash command"))
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    context.client()?;
    let source = parser
        .shell_source
        .ok_or_else(|| anyhow::anyhow!("missing Bash source"))?;
    if source.trim().is_empty() {
        return Ok(vec!["usage: !<bash command>".into()].into());
    }
    let status = crate::local_shell::run(source).context("cannot run local command")?;
    if !status.success() {
        bail!("bash exited with {status}");
    }
    Ok(Vec::new().into())
}
