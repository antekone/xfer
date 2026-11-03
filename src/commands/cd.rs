/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Change backend working directory (default: initial directory).
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use crate::daemon::{Daemon, SessionInfo};
use anyhow::Result;

pub(super) struct Cd;
impl Command for Cd {
    fn names(&self) -> &'static [&'static str] {
        &["cd"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "cd [directory]",
            "change backend working directory (default: initial directory)",
        ))
    }
    fn completion(&self) -> Completion {
        Completion::RemotePath {
            kind: crate::ipc::pb::CompletionKind::RemoteDirectory,
            multiple_arguments: false,
        }
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> CommandResult {
    let mut words = parser.words();
    let path = words.next().map(str::to_owned);
    if words.next().is_some() {
        return vec!["usage: cd [directory]".into()].into();
    }
    let connection = app.connection.clone();
    let session = app.session.clone();
    let snapshot = std::sync::Arc::new(std::sync::Mutex::new(None));
    let id = app.jobs.start_retrying_operation(
        format!("cd {}", path.as_deref().unwrap_or("")),
        move |_| {
            let connection = connection.clone();
            let session = session.clone();
            let path = path.clone();
            let snapshot = snapshot.clone();
            async move {
                let mut active = connection.lock().await;
                let active = active.as_mut().ok_or_else(|| {
                    crate::error::NonRetryableError("not connected; use connect first".into())
                })?;
                let target = snapshot_directory(active.as_ref(), path.as_deref(), &snapshot)?;
                active.change_directory(&target).await?;
                let cwd = active.pwd()?;
                *session.lock().expect("session state poisoned") =
                    SessionInfo::connected(active.as_ref());
                Ok(cwd)
            }
        },
    );
    CommandResult::JobStarted(id)
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser))
}

pub(crate) fn snapshot_directory(
    connection: &dyn crate::backend::Connection,
    path: Option<&str>,
    snapshot: &std::sync::Mutex<Option<String>>,
) -> Result<String> {
    let mut saved = snapshot.lock().expect("directory snapshot poisoned");
    if saved.is_none() {
        *saved = Some(connection.resolve_directory(path)?);
    }
    Ok(saved.as_ref().unwrap().clone())
}
