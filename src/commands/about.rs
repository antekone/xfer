/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Application and build information embedded at compile time.
use super::{Command, CommandContext, CommandResult, Completion, Handler, ParserContext};
use anyhow::Result;

pub(super) struct About;
impl Command for About {
    fn names(&self) -> &'static [&'static str] {
        &["about"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("about", "show application and build information"))
    }
    fn completion(&self) -> Completion {
        Completion::None
    }
}

fn lines(commit: Option<&str>, date: Option<&str>) -> Vec<String> {
    let metadata = |value: Option<&str>| match value {
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => "unknown".to_owned(),
    };
    vec![
        format!("xfer {}", env!("CARGO_PKG_VERSION")),
        "Interactive terminal-based remote file-transfer client".into(),
        format!("License: {}", env!("CARGO_PKG_LICENSE")),
        "Homepage: https://anadoxin.org/xfer".into(),
        format!("Git commit: {}", metadata(commit)),
        format!("Build date: {}", metadata(date)),
    ]
}

fn execute(_context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    if !parser.arguments().is_empty() {
        return Ok(vec!["usage: about".into()].into());
    }
    Ok(lines(
        option_env!("XFER_GIT_COMMIT"),
        option_env!("XFER_BUILD_DATE"),
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::Daemon;

    #[test]
    fn about_reports_package_and_build_information_without_connection() {
        let mut app = Daemon::bootstrap().unwrap();
        let result = app.command("about");
        assert_eq!(result[0], format!("xfer {}", env!("CARGO_PKG_VERSION")));
        assert!(result.contains(&format!("License: {}", env!("CARGO_PKG_LICENSE"))));
        assert!(result.contains(&"Homepage: https://anadoxin.org/xfer".into()));
        assert!(result.contains(&format!("Git commit: {}", env!("XFER_GIT_COMMIT"))));
        assert!(result.contains(&format!("Build date: {}", env!("XFER_BUILD_DATE"))));
        assert_eq!(app.command("about unexpected"), ["usage: about"]);
        assert_eq!(crate::commands::completion_for("about"), Completion::None);
    }

    #[test]
    fn missing_build_metadata_is_reported_as_unknown() {
        let result = lines(None, Some(""));
        assert!(result.contains(&"Git commit: unknown".into()));
        assert!(result.contains(&"Build date: unknown".into()));
    }
}
