/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Show daemon, connection, configuration or job details.
use super::{Command, CommandContext, CommandResult, Handler, ParserContext};
use crate::{
    core::{JobState, Progress},
    daemon::Daemon,
};
use anyhow::Result;

pub(super) struct Status;
impl Command for Status {
    fn names(&self) -> &'static [&'static str] {
        &["status"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some((
            "status [job-id]",
            "show daemon, connection, configuration or job details",
        ))
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match words.next() {
        None => {
            let info = app.session_info();
            let mut lines = vec![
                format!("Daemon PID: {}", info.daemon_pid),
                format!(
                    "Backend: {}",
                    info.protocol.as_deref().unwrap_or("disconnected")
                ),
                format!("CWD: {}", info.cwd.as_deref().unwrap_or("-")),
                format!("Local CWD: {}", app.local_directory.display()),
                format!(
                    "Running jobs: {}",
                    app.jobs
                        .jobs
                        .values()
                        .filter(|job| job.state == JobState::Running)
                        .count()
                ),
                app.config_file.status("Config file"),
            ];
            if let Some(config) = &app.config_override {
                lines.push(config.status("Config override (XFER_CONFIG)"));
            }
            if let Some(path) = &app.startup_file {
                lines.push(format!("RC file: {}", path.display()));
            }
            let bookmarks = std::path::Path::new(app.settings.text("bookmarks:file").unwrap());
            let bookmarks = std::path::absolute(bookmarks).unwrap_or_else(|_| bookmarks.to_owned());
            lines.push(format!("Bookmarks file: {}", bookmarks.display()));
            lines
        }
        Some(id) => match id.parse().ok().and_then(|id| app.jobs.jobs.get(&id)) {
            Some(job) => match job.progress.borrow().clone() {
                Progress::Transfer(mut stats) => {
                    stats.elapsed_ms =
                        job.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    vec![format!(
                        "[{}] {:?}{} TRANSFER {}",
                        job.id,
                        job.state,
                        if job.raw_output { " RAW" } else { "" },
                        serde_json::to_string(&stats).unwrap()
                    )]
                }
                Progress::Determinate {
                    done,
                    total,
                    message,
                } => vec![format!(
                    "[{}] {:?}{} {done}/{total} {message}",
                    job.id,
                    job.state,
                    if job.raw_output { " RAW" } else { "" }
                )],
                Progress::Indeterminate { message } => {
                    vec![format!(
                        "[{}] {:?}{} {message}",
                        job.id,
                        job.state,
                        if job.raw_output { " RAW" } else { "" }
                    )]
                }
            },
            None => vec!["no such job".into()],
        },
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser).into())
}
