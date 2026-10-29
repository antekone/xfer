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

fn execute_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> CommandResult {
    let mut words = parser.words();
    match words.next() {
        None => (match crate::bookmarks::load(std::path::Path::new(
            app.settings.text("bookmarks:file").unwrap(),
        )) {
            Ok(entries) if entries.is_empty() => vec!["no bookmarks".into()],
            Ok(entries) => {
                use unicode_width::UnicodeWidthStr;
                let rows = entries
                    .into_iter()
                    .map(|(alias, bookmark)| {
                        let name = alias
                            .chars()
                            .flat_map(|ch| {
                                if ch.is_control() {
                                    ch.escape_default().collect::<Vec<_>>()
                                } else {
                                    vec![ch]
                                }
                            })
                            .collect::<String>();
                        (name, bookmark.address())
                    })
                    .collect::<Vec<_>>();
                let width = rows.iter().map(|(name, _)| name.width()).max().unwrap_or(0);
                rows.into_iter()
                    .map(|(name, address)| {
                        format!("{name}{}{address}", " ".repeat(width - name.width() + 2))
                    })
                    .collect()
            }
            Err(error) => vec![error.to_string()],
        })
        .into(),
        Some("edit") => {
            let path = std::path::Path::new(app.settings.text("bookmarks:file").unwrap());
            match crate::bookmarks::prepare_for_editor(path) {
                Ok(()) => CommandResult::EditBookmarks(path.to_owned()),
                Err(error) => vec![format!("cannot prepare bookmarks: {error}")].into(),
            }
        }
        Some("reload") => (match crate::bookmarks::load(std::path::Path::new(
            app.settings.text("bookmarks:file").unwrap(),
        )) {
            Ok(_) => vec!["bookmarks validated; connections use the latest file".into()],
            Err(error) => vec![error.to_string()],
        })
        .into(),
        _ => vec!["usage: bookmark [edit | reload]".into()].into(),
    }
}

fn execute(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(execute_lines(context.daemon()?, parser))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn names_and_safe_addresses_align_in_two_columns() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bookmarks.toml");
        std::fs::write(&path, "[short]\nurl = 'sftp://user:secret@example.com/path?token=secret#hidden'\n[\"日本語\"]\nurl = 's3://bucket'\n").unwrap();
        let mut app = Daemon::bootstrap().unwrap();
        app.settings
            .set("bookmarks:file", path.to_str().unwrap())
            .unwrap();
        let lines = app.command("bookmark");
        assert_eq!(
            lines,
            vec!["short   sftp://example.com/path", "日本語  s3://bucket"]
        );
        assert!(!lines.join(" ").contains("secret"));
    }
}
