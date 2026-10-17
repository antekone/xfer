//! Protocol-internal spool reads and acknowledgement.
use super::{small, CommandContext, CommandResult, ParserContext, SmallCommand};
use crate::{core::JobState, daemon::Daemon};
use anyhow::Result;
use std::fs;
pub(super) static READ_OUTPUT: SmallCommand = small(&["read-output"], None, read_output);

fn read_output_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    let id = words.next().and_then(|value| value.parse().ok());
    let offset = words
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    match id.and_then(|id| app.jobs.jobs.get(&id)) {
        Some(job) => match job.output.read_bytes(offset) {
            Ok(bytes) => vec![format!(
                "CHUNK {} {}",
                offset + bytes.len() as u64,
                serde_json::to_string(&bytes).unwrap()
            )],
            Err(error) => vec![format!("cannot read job output: {error}")],
        },
        None => vec!["no such job".into()],
    }
}

fn read_output(
    context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> Result<CommandResult> {
    Ok(read_output_lines(context.daemon()?, parser).into())
}
pub(super) static OUTPUT: SmallCommand = small(&["output"], None, output);

fn output_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    let id = words.next().and_then(|value| value.parse().ok());
    let offset = words
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    match id.and_then(|id| app.jobs.jobs.get(&id)) {
        Some(job) => {
            let (next, data) = job.output.read_chunk(offset, 64 * 1024);
            let mut lines = vec![format!("OFFSET {next}")];
            lines.extend(data.lines().map(str::to_owned));
            lines
        }
        None => vec!["no such job".into()],
    }
}

fn output(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(output_lines(context.daemon()?, parser).into())
}
pub(super) static ACK_OUTPUT: SmallCommand = small(&["ack-output"], None, ack_output);

fn ack_output_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match words.next().and_then(|value| value.parse().ok()) {
        Some(id) => match app.jobs.jobs.get(&id) {
            Some(job) if job.state == JobState::Running => {
                vec!["cannot acknowledge output of a running job".into()]
            }
            Some(job) => match fs::remove_file(job.output.path()) {
                Ok(()) => {
                    app.jobs.jobs.remove(&id);
                    vec!["output acknowledged".into()]
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    app.jobs.jobs.remove(&id);
                    vec!["output acknowledged".into()]
                }
                Err(error) => vec![format!("cannot remove output spool: {error}")],
            },
            None => vec!["no such job".into()],
        },
        None => vec!["usage: ack-output <job-id>".into()],
    }
}

fn ack_output(
    context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> Result<CommandResult> {
    Ok(ack_output_lines(context.daemon()?, parser).into())
}
