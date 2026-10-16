//! Prompt-local editing. Cursor and viewport offsets are always UTF-8 boundaries.
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Edit {
    Ignored,
    Moved,
    Changed,
}

pub(super) struct CommandLine {
    text: String,
    cursor: usize,
    view_start: usize,
}
impl CommandLine {
    pub(super) fn new(text: String) -> Self {
        let cursor = text.len();
        Self {
            text,
            cursor,
            view_start: 0,
        }
    }
    pub(super) fn as_str(&self) -> &str {
        &self.text
    }
    pub(super) fn prefix(&self) -> &str {
        &self.text[..self.cursor]
    }
    pub(super) fn into_string(self) -> String {
        self.text
    }
    pub(super) fn replace(&mut self, text: String) {
        *self = Self::new(text);
    }
    pub(super) fn clear(&mut self) {
        self.replace(String::new());
    }

    pub(super) fn key(&mut self, key: KeyEvent) -> Edit {
        if key.kind == KeyEventKind::Release {
            return Edit::Ignored;
        }
        let control = key.modifiers == KeyModifiers::CONTROL;
        match key.code {
            KeyCode::Left => self.cursor = self.previous(),
            KeyCode::Right => self.cursor = self.next(),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.text.len(),
            KeyCode::Char('a') if control => self.cursor = 0,
            KeyCode::Char('e') if control => self.cursor = self.text.len(),
            KeyCode::Backspace if self.cursor > 0 => {
                let start = self.previous();
                self.remove(start, self.cursor);
                return Edit::Changed;
            }
            KeyCode::Char('w') if control && self.cursor > 0 => {
                self.remove(previous_word_start(self.prefix()), self.cursor);
                return Edit::Changed;
            }
            KeyCode::Delete | KeyCode::Char('d') if key.code == KeyCode::Delete || control => {
                if self.cursor < self.text.len() {
                    self.remove(self.cursor, self.next());
                    return Edit::Changed;
                }
            }
            KeyCode::Char(ch)
                if (key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT)
                    && !ch.is_control() =>
            {
                self.text.insert(self.cursor, ch);
                self.cursor += ch.len_utf8();
                return Edit::Changed;
            }
            _ => return Edit::Ignored,
        }
        self.view_start = self.view_start.min(self.cursor);
        Edit::Moved
    }
    fn previous(&self) -> usize {
        self.prefix()
            .char_indices()
            .next_back()
            .map_or(0, |(index, _)| index)
    }
    fn next(&self) -> usize {
        self.text[self.cursor..]
            .chars()
            .next()
            .map_or(self.cursor, |ch| self.cursor + ch.len_utf8())
    }
    fn remove(&mut self, start: usize, end: usize) {
        self.text.replace_range(start..end, "");
        self.cursor = start;
        self.view_start = self.view_start.min(start);
    }
    pub(super) fn paste(&mut self, text: &str) {
        let clean = text
            .chars()
            .filter_map(|ch| match ch {
                '\r' | '\n' | '\t' => Some(' '),
                ch if !ch.is_control() => Some(ch),
                _ => None,
            })
            .collect::<String>();
        self.text.insert_str(self.cursor, &clean);
        self.cursor += clean.len();
    }

    /// Replace the completed prefix and the remainder of its active token,
    /// preserving subsequent arguments and placing the caret after completion.
    pub(super) fn complete(&mut self, prefix: String) {
        if prefix == self.prefix() {
            return;
        }
        let end = self.token_end();
        self.text.replace_range(..end, &prefix);
        self.cursor = prefix.len();
        self.view_start = 0;
    }
    fn token_end(&self) -> usize {
        let mut quote = None;
        let mut escaped = false;
        for (index, ch) in self.text.char_indices() {
            if escaped {
                escaped = false;
                continue;
            }
            if quote == Some('\'') {
                if ch == '\'' {
                    quote = None;
                }
            } else if ch == '\\' {
                escaped = true;
            } else if let Some(current) = quote {
                if ch == current {
                    quote = None;
                }
            } else if ch == '\'' || ch == '"' {
                quote = Some(ch);
            } else if index >= self.cursor && ch.is_whitespace() {
                return index;
            }
        }
        self.text.len()
    }

    /// Keep a single-row viewport, leaving a spare terminal column to prevent
    /// autowrap. Count display cells (not bytes), including wide Unicode text.
    pub(super) fn frame(&mut self, prompt: &str, columns: u16, clear: bool) -> String {
        let limit = columns.saturating_sub(1) as usize;
        let prompt_budget = columns.saturating_sub(3).min(columns / 2 + 1);
        let prompt = crate::fit_status_line(prompt, prompt_budget);
        let prompt_width = prompt.width();
        let available = limit.saturating_sub(prompt_width);
        self.view_start = self.view_start.min(self.cursor);
        let mut before = self.text[self.view_start..self.cursor]
            .chars()
            .map(cells)
            .sum::<usize>();
        while before > available.saturating_sub(1) && self.view_start < self.cursor {
            let ch = self.text[self.view_start..].chars().next().unwrap();
            self.view_start += ch.len_utf8();
            before -= cells(ch);
        }
        let mut used = 0;
        let visible = self.text[self.view_start..]
            .chars()
            .take_while(|&ch| {
                let width = cells(ch);
                if used + width > available {
                    return false;
                }
                used += width;
                true
            })
            .collect::<String>();
        let visible = printable(&visible);
        let caret = prompt_width + printable(&self.text[self.view_start..self.cursor]).width();
        let mut output = if clear {
            "\x1b[2J\x1b[H".to_owned()
        } else {
            String::new()
        };
        output.push_str(&format!("\r\x1b[2K{prompt}{visible}\r"));
        if caret > 0 {
            output.push_str(&format!("\x1b[{caret}C"));
        }
        output
    }
}

pub(super) fn previous_word_start(text: &str) -> usize {
    text.trim_end()
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(0, |(index, ch)| index + ch.len_utf8())
}
fn cells(ch: char) -> usize {
    if ch.is_control() {
        ch.escape_default().count()
    } else {
        ch.width().unwrap_or(0)
    }
}
fn printable(text: &str) -> String {
    let mut output = String::new();
    for ch in text.chars() {
        if ch.is_control() {
            output.extend(ch.escape_default());
        } else {
            output.push(ch);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    fn press(line: &mut CommandLine, code: KeyCode) -> Edit {
        line.key(KeyEvent::new(code, KeyModifiers::NONE))
    }
    fn control(line: &mut CommandLine, ch: char) -> Edit {
        line.key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL))
    }

    #[test]
    fn insertion_and_backspace_work_at_the_caret_on_utf8_boundaries() {
        let mut line = CommandLine::new("ac".into());
        press(&mut line, KeyCode::Left);
        press(&mut line, KeyCode::Char('β'));
        assert_eq!(line.as_str(), "aβc");
        assert_eq!(line.prefix(), "aβ");
        press(&mut line, KeyCode::Left);
        assert_eq!(line.prefix(), "a");
        press(&mut line, KeyCode::Right);
        press(&mut line, KeyCode::Backspace);
        assert_eq!(line.as_str(), "ac");
        press(&mut line, KeyCode::Char('界'));
        assert_eq!(line.as_str(), "a界c");
        press(&mut line, KeyCode::End);
        press(&mut line, KeyCode::Backspace);
        assert_eq!(line.as_str(), "a界");
        press(&mut line, KeyCode::Home);
        assert_eq!(press(&mut line, KeyCode::Backspace), Edit::Ignored);
        press(&mut line, KeyCode::Right);
        press(&mut line, KeyCode::Delete);
        assert_eq!(line.as_str(), "a");
        press(&mut line, KeyCode::Home);
        control(&mut line, 'd');
        assert_eq!(line.as_str(), "");
    }

    #[test]
    fn movement_home_end_and_control_a_e_are_clamped_and_equivalent() {
        let mut line = CommandLine::new("résumé界".into());
        for _ in 0..20 {
            press(&mut line, KeyCode::Left);
        }
        assert_eq!(line.cursor, 0);
        for _ in 0..20 {
            press(&mut line, KeyCode::Right);
        }
        assert_eq!(line.cursor, line.text.len());
        control(&mut line, 'a');
        assert_eq!(line.cursor, 0);
        control(&mut line, 'e');
        assert_eq!(line.cursor, line.text.len());
        press(&mut line, KeyCode::Home);
        assert_eq!(line.cursor, 0);
        press(&mut line, KeyCode::End);
        assert_eq!(line.cursor, line.text.len());
    }

    #[test]
    fn word_erase_and_paste_preserve_text_after_the_caret() {
        let mut line = CommandLine::new("get first second   tail".into());
        line.cursor = "get first second   ".len();
        control(&mut line, 'w');
        assert_eq!(line.as_str(), "get first tail");
        assert_eq!(line.prefix(), "get first ");
        line.paste("photo\nalbum\t界\x1b");
        assert_eq!(line.as_str(), "get first photo album 界tail");
        assert!(!line.text.chars().any(char::is_control));
        assert_eq!(line.prefix(), "get first photo album 界");
        assert_eq!(press(&mut line, KeyCode::Enter), Edit::Ignored);
    }

    #[test]
    fn recalled_or_buffered_lines_start_with_the_caret_at_the_end_and_release_is_ignored() {
        let mut line = CommandLine::new("buffered 界".into());
        assert_eq!(line.cursor, line.text.len());
        press(&mut line, KeyCode::Home);
        line.replace("history résumé".into());
        assert_eq!(line.cursor, line.text.len());
        assert_eq!(
            line.key(KeyEvent {
                kind: KeyEventKind::Release,
                ..KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)
            }),
            Edit::Ignored
        );
        assert_eq!(line.as_str(), "history résumé");
        line.clear();
        assert_eq!(line.cursor, 0);
        assert!(line.as_str().is_empty());
    }

    #[test]
    fn completion_replaces_the_active_token_but_not_later_arguments() {
        for (text, prefix, completed, expected) in [
            ("status arg", "st", "status", "status arg"),
            (
                "get 'photo alb' next.txt",
                "get 'pho",
                "get 'photo album/'",
                "get 'photo album/' next.txt",
            ),
            (
                "get photo\\ album next.txt",
                "get photo\\ alb",
                "get 'photo album/'",
                "get 'photo album/' next.txt",
            ),
            (
                "get \"photo alb\" next.txt",
                "get \"pho",
                "get 'photo album/'",
                "get 'photo album/' next.txt",
            ),
        ] {
            let mut line = CommandLine::new(text.into());
            line.cursor = prefix.len();
            line.complete(completed.into());
            assert_eq!(line.as_str(), expected);
            assert_eq!(line.prefix(), completed);
        }
        let mut line = CommandLine::new("get unfinished rest".into());
        line.cursor = "get un".len();
        line.complete(line.prefix().into());
        assert_eq!(line.as_str(), "get unfinished rest");
    }

    #[test]
    fn redraw_positions_the_caret_by_display_cells_and_clear_preserves_the_line() {
        let mut line = CommandLine::new("a界b".into());
        press(&mut line, KeyCode::Left);
        let plain = line.frame("xfer> ", 80, false);
        assert_eq!(plain, "\r\x1b[2Kxfer> a界b\r\x1b[9C");
        let cleared = line.frame("xfer> ", 80, true);
        assert_eq!(cleared, format!("\x1b[2J\x1b[H{plain}"));
        assert_eq!(line.prefix(), "a界");
        assert_eq!(line.as_str(), "a界b");
        let mut history = CommandLine::new("a\x1b[2J".into());
        let frame = history.frame("xfer> ", 80, false);
        assert!(frame.contains("a\\u{1b}[2J"));
        assert!(!frame.contains("\x1b[2J"));
    }

    #[test]
    fn long_lines_and_narrow_resizes_scroll_without_wrap_or_corrupting_text() {
        let text = "résumé/界/".repeat(50);
        let mut line = CommandLine::new(text.clone());
        for width in 0..100 {
            let frame = line.frame("xfer[sftp:/a/very/long/directory/]> ", width, false);
            let visible = frame
                .strip_prefix("\r\x1b[2K")
                .unwrap()
                .split('\r')
                .next()
                .unwrap();
            assert!(
                visible.width() <= width.saturating_sub(1) as usize,
                "width {width}"
            );
            assert!(line.text.is_char_boundary(line.view_start));
            assert!(line.view_start <= line.cursor);
            assert_eq!(line.text, text);
        }
        press(&mut line, KeyCode::Home);
        let frame = line.frame("xfer> ", 30, false);
        assert!(frame.contains("xfer> résumé/界/"));
        assert_eq!(line.view_start, 0);
    }
}
