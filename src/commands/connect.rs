//! Connection setup, bookmarks, credentials and retried backend initialization.
use super::{Command, CommandContext, CommandResult, Handler, ParserContext};
use crate::core::{Daemon, JobId, SessionInfo};
use anyhow::{bail, Result};

pub(super) struct Connect;
impl Command for Connect {
    fn names(&self) -> &'static [&'static str] {
        &["connect", "open"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("connect <url|name>", "connect using a protocol or bookmark"))
    }
}
fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let CommandContext::Daemon { app, password } = context else {
        bail!("connect must run in the daemon");
    };
    let Some(target) = parser.words().next() else {
        return Ok(vec!["usage: connect <protocol://target | bookmark>".into()].into());
    };
    let lines = match start(app, target, password.take()) {
        Ok(id) => vec![format!("JOB {id}")],
        Err(error) if error.is::<crate::credentials::PasswordRequired>() => {
            vec![format!(
                "PASSWORD_REQUIRED {}",
                serde_json::to_string(target).unwrap()
            )]
        }
        Err(error) => return Err(error),
    };
    Ok(lines.into())
}

fn start(
    app: &mut Daemon,
    target: &str,
    password: Option<crate::credentials::Password>,
) -> Result<JobId> {
    let bookmark = if target.contains("://") {
        None
    } else {
        Some(
            crate::bookmarks::load(std::path::Path::new(
                app.settings.text("bookmarks:file").unwrap(),
            ))?
            .remove(target)
            .ok_or_else(|| anyhow::anyhow!("unknown bookmark: {target}"))?,
        )
    };
    let token = bookmark
        .as_ref()
        .map(|value| value.protocol.as_str())
        .or_else(|| target.split_once("://").map(|(token, _)| token))
        .unwrap_or("");
    if password.is_some() && token != "sftp" {
        anyhow::bail!("interactive passwords are only supported for sftp");
    }
    let backend = app
        .backends
        .get(token)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no backend for protocol: {token}"))?;
    if bookmark
        .as_ref()
        .is_some_and(|bookmark| bookmark.password_interactive)
        && password.is_none()
    {
        return Err(crate::credentials::PasswordRequired.into());
    }
    let cache_options = crate::cache::CacheOptions::from_settings(&app.settings)?;
    let options = crate::backend::ConnectOptions {
        target: target.to_owned(),
        bookmark,
        settings: app.settings.clone(),
        password,
    };
    let connection = app.connection.clone();
    let session = app.session.clone();
    Ok(app
        .jobs
        .start_retrying_operation(format!("connect {target}"), move |_| {
            let connection = connection.clone();
            let backend = backend.clone();
            let options = options.clone();
            let cache_options = cache_options.clone();
            let session = session.clone();
            async move {
                // Serialize changes per attempt; the lock is released during backoff.
                let mut active = connection.lock().await;
                let new = backend.connect(options).await?;
                let message = format!("connected {}", new.protocol());
                let info = SessionInfo::connected(new.as_ref());
                *active = Some(Box::new(crate::cache::CachedConnection::new(
                    new,
                    cache_options,
                )));
                *session.lock().expect("session state poisoned") = info;
                Ok(message)
            }
        }))
}
