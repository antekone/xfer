/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Local Bash escapes. Bash receives untouched source and owns the terminal.
use super::{Command, CommandContext, CommandResult, ExecutionSite, Handler, ParserContext};
use anyhow::{bail, Result};

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
        Some(("!<command>", "run local Bash command in Local CWD"))
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
    let status = context
        .client()?
        .run_shell(source)
        .map_err(|error| anyhow::anyhow!("cannot run local command: {error:#}"))?;
    if !status.success() {
        bail!("bash exited with {status}");
    }
    Ok(Vec::new().into())
}
