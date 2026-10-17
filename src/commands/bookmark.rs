//! List or edit toml bookmarks.
use super::{Command, CommandContext, CommandResult, Handler, ParserContext};
use crate::daemon::Daemon;
use anyhow::Result;

pub(super) struct Bookmark;
impl Command for Bookmark {
    fn names(&self) -> &'static [&'static str] {
        &["bookmark"]
    }
    fn handler(&self) -> Handler {
        execute
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        Some(("bookmark [edit | reload]", "list or edit TOML bookmarks"))
    }
}

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match words.next() {
        None => match crate::bookmarks::load(std::path::Path::new(
            app.settings.text("bookmarks:file").unwrap(),
        )) {
            Ok(entries) if entries.is_empty() => vec!["no bookmarks".into()],
            Ok(entries) => entries
                .into_iter()
                .map(|(alias, bookmark)| bookmark.summary(&alias))
                .collect(),
            Err(error) => vec![error.to_string()],
        },
        Some("edit") => {
            let path = std::path::Path::new(app.settings.text("bookmarks:file").unwrap());
            match crate::bookmarks::prepare_for_editor(path) {
                Ok(()) => vec![format!("BOOKMARK_EDIT {}", path.display())],
                Err(error) => vec![format!("cannot prepare bookmarks: {error}")],
            }
        }
        Some("reload") => match crate::bookmarks::load(std::path::Path::new(
            app.settings.text("bookmarks:file").unwrap(),
        )) {
            Ok(_) => vec!["bookmarks validated; connections use the latest file".into()],
            Err(error) => vec![error.to_string()],
        },
        _ => vec!["usage: bookmark [edit | reload]".into()],
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser).into())
}
