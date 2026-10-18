//! Terminal line input: UTF-8-safe editing and viewport rendering, prompt
//! reading, completion, history, typeahead and password input. Command dispatch
//! and job coordination stay in the attached client, not this module.
use super::Connection;
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use xfer::{
    commands::{self, Completion},
    ipc::{self, pb, Op, Reply},
};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, PartialEq, Eq)]
enum Edit {
    Ignored,
    Moved,
    Changed,
}

struct CommandLine {
    text: String,
    cursor: usize,
    view_start: usize,
}
impl CommandLine {
    fn new(text: String) -> Self {
        let cursor = text.len();
        Self {
            text,
            cursor,
            view_start: 0,
        }
    }
    fn as_str(&self) -> &str {
        &self.text
    }
    fn prefix(&self) -> &str {
        &self.text[..self.cursor]
    }
    fn into_string(self) -> String {
        self.text
    }
    fn replace(&mut self, text: String) {
        *self = Self::new(text);
    }
    fn clear(&mut self) {
        self.replace(String::new());
    }

    fn key(&mut self, key: KeyEvent) -> Edit {
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
    fn paste(&mut self, text: &str) {
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
    fn complete(&mut self, prefix: String) {
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
    fn frame(&mut self, prompt: &str, columns: u16, clear: bool) -> String {
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

fn previous_word_start(text: &str) -> usize {
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

fn history_persist_allowed(path: Option<&str>) -> bool {
    if path == None || path == Some("session") || path == Some("/dev/null") {
        false
    } else {
        true
    }
}

pub(super) fn history_persist_load(path: Option<&str>) -> Vec<String> {
    if !history_persist_allowed(path) {
        Vec::new()
    } else {
        // can_persist_history will check if path is None
        fs::read_to_string(path.unwrap())
            .map(|data| data.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }
}

pub(super) fn history_persist_append(path: Option<&str>, command: &str) {
    let trimmed_command = command.trim();
    if !history_persist_allowed(path) || trimmed_command.len() == 0 {
        return;
    }

    let path = PathBuf::from(path.unwrap());
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let _ = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| writeln!(file, "{trimmed_command}"));
}

fn erase_previous_word(command: &mut String) {
    command.truncate(previous_word_start(command));
}

fn common_completion_prefix(choices: &[String]) -> &str {
    let Some(first) = choices.first() else {
        return "";
    };
    let mut length = first.len();
    for choice in &choices[1..] {
        length = first[..length]
            .chars()
            .zip(choice.chars())
            .take_while(|(left, right)| left == right)
            .map(|(ch, _)| ch.len_utf8())
            .sum();
        if length == 0 {
            break;
        }
    }
    &first[..length]
}

fn path_completion_context(command: &str) -> Option<(String, String, pb::CompletionKind)> {
    let command = command.trim_start();
    let verb = command.split_whitespace().next()?;
    let request = match commands::completion_for(verb) {
        Completion::RemotePath { kind, .. } | Completion::LocalPath { kind, .. } => kind,
        _ => return None,
    };
    let argument = command.strip_prefix(verb)?.trim_start();
    let mut tokens = shlex::split(argument)
        .or_else(|| shlex::split(&format!("{argument}'")))
        .or_else(|| shlex::split(&format!("{argument}\"")))?;
    let mut quote = None;
    let mut escaped = false;
    let mut boundary = true;
    for ch in argument.chars() {
        if escaped {
            escaped = false;
            boundary = false;
            continue;
        }
        if quote == Some('\'') {
            if ch == '\'' {
                quote = None;
            }
            boundary = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            boundary = false;
            continue;
        }
        if let Some(current) = quote {
            if ch == current {
                quote = None;
            }
            boundary = false;
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            boundary = false;
        } else {
            boundary = ch.is_whitespace();
        }
    }
    let path = if boundary {
        String::new()
    } else {
        tokens.pop().unwrap_or_default()
    };
    if !commands::accepts_path_completion(verb, &tokens, &path) {
        return None;
    }
    let mut head = verb.to_owned();
    for token in tokens {
        head.push(' ');
        head.push_str(&shlex::try_quote(&token).ok()?);
    }
    Some((head, path, request))
}

fn complete_path_command(client: &mut Connection, command: &mut String) -> Result<Vec<String>> {
    let Some((head, path, method)) = path_completion_context(command) else {
        return Ok(Vec::new());
    };
    let result = client.call(Op::Complete(pb::CompletionRequest {
        kind: method as i32,
        input: path.clone(),
    }));
    let mut notices = Vec::new();
    let mut choices = Vec::new();
    let mut complete_set = true;
    match result {
        Ok(Reply::Completion(result)) => {
            choices.extend(
                result
                    .candidates
                    .into_iter()
                    .filter(|path| !path.chars().any(char::is_control)),
            );
            if result.truncated {
                complete_set = false;
                notices.push("completion listing limited; type a longer prefix".into());
            }
        }
        Ok(_) => return client.unexpected(),
        Err(error) if error.is::<ipc::RemoteError>() => notices.push(
            error
                .to_string()
                .chars()
                .map(|ch| if ch.is_control() { ' ' } else { ch })
                .collect(),
        ),
        Err(error) => return Err(error),
    }
    let common = common_completion_prefix(&choices);
    if complete_set && common.starts_with(&path) && common.len() > path.len() {
        // Compute on decoded paths, THEN quote: quotes themselves are not part
        // of the directory name and must not affect the shared prefix.
        *command = format!("{head} {}", shlex::try_quote(common)?);
    }
    if choices.len() > 1 || (!complete_set && !choices.is_empty()) {
        let labels = choices
            .iter()
            .take(20)
            .map(|path| shlex::try_quote(path).map(|quoted| quoted.into_owned()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        notices.push(labels.join("  "));
        if choices.len() > 20 {
            notices.push(format!(
                "... {} more matches; type a longer prefix",
                choices.len() - 20
            ));
        }
    }
    Ok(notices)
}

/// Completion operates on the prefix at the caret; the editor keeps later
/// arguments intact and replaces only the remainder of the active token.
fn complete_command(client: &mut Connection, command: &mut String) -> Result<Vec<String>> {
    let verb = command.split_whitespace().next().unwrap_or("");
    let completion = commands::completion_for(verb);
    if matches!(
        completion,
        Completion::RemotePath { .. } | Completion::LocalPath { .. }
    ) && command.contains(char::is_whitespace)
    {
        return complete_path_command(client, command);
    }
    let matches = if !command.contains(char::is_whitespace) {
        commands::visible_names()
            .filter(|name| name.starts_with(command.as_str()))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    } else if completion == Completion::Settings {
        let choices = client
            .settings(None)?
            .into_iter()
            .map(|setting| setting.name)
            .collect::<Vec<_>>();
        let prefix = command.trim_start().strip_prefix(verb).unwrap_or("").trim();
        choices
            .into_iter()
            .filter(|name| name.starts_with(prefix))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if !matches.is_empty() {
        let common = common_completion_prefix(&matches);
        if let Some((verb, prefix)) = command.trim_start().split_once(char::is_whitespace) {
            if common.len() > prefix.trim().len() {
                *command = format!("{verb} {common}");
            }
        } else if common.len() > command.len() || matches.len() == 1 {
            *command = common.to_owned();
            // A unique initial command is complete: end the word so the next
            // keypress starts an argument, like shell command completion.
            if matches.len() == 1 {
                command.push(' ');
            }
        }
    }
    Ok(if matches.len() > 1 {
        vec![matches.join("  ")]
    } else {
        Vec::new()
    })
}

const MAX_TYPEAHEAD_BYTES: usize = 16 * 1024;

#[derive(Default)]
pub(super) struct Typeahead {
    text: String,
}
impl Typeahead {
    /// Discard buffered input before a credential prompt without leaving its
    /// contents in the allocation. Foreground job control shares this buffer.
    pub(super) fn clear(&mut self) {
        self.text.zeroize();
    }
    fn push(&mut self, ch: char) {
        if !ch.is_control() && self.text.len() + ch.len_utf8() <= MAX_TYPEAHEAD_BYTES {
            self.text.push(ch);
        }
    }
    pub(super) fn capture(&mut self, event: Event) {
        match event {
            Event::Key(key) if key.kind != KeyEventKind::Release => match key.code {
                KeyCode::Char(ch)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    self.push(ch)
                }
                KeyCode::Backspace => {
                    self.text.pop();
                }
                KeyCode::Char('w') if key.modifiers == KeyModifiers::CONTROL => {
                    erase_previous_word(&mut self.text)
                }
                // Enter must never submit an unseen command. Ctrl-C/Z are
                // handled by job control, not saved as input characters.
                _ => {}
            },
            Event::Paste(text) => {
                for ch in text.chars() {
                    match ch {
                        '\r' | '\n' | '\t' => self.push(' '),
                        _ => self.push(ch),
                    }
                }
            }
            _ => {}
        }
    }
    fn take(&mut self) -> String {
        std::mem::take(&mut self.text)
    }
}

fn redraw_command(command: &mut CommandLine, prompt: &str, clear: bool) -> Result<()> {
    let columns = crossterm::terminal::size()
        .map(|(width, _)| width)
        .unwrap_or(80);

    print!("{}", command.frame(prompt, columns, clear));
    io::stdout().flush()?;

    Ok(())
}

pub(super) fn read_attached_command(
    client: &mut Connection,
    prompt: &str,
    history: &mut Vec<String>,
    typeahead: &mut Typeahead,
) -> Result<Option<String>> {
    enable_raw_mode()?;

    let _raw_input = RawInputGuard;
    let mut command = CommandLine::new(typeahead.take());
    let mut history_cursor = None;

    redraw_command(&mut command, prompt, false)?;

    loop {
        let key = match event::read()? {
            Event::Key(key) if key.kind != KeyEventKind::Release => key,
            Event::Paste(text) => {
                history_cursor = None;
                command.paste(&text);
                redraw_command(&mut command, prompt, false)?;
                continue;
            }
            Event::Resize(..) => {
                redraw_command(&mut command, prompt, true)?;
                continue;
            }
            _ => continue,
        };
        let mut clear = false;
        match key.code {
            KeyCode::Enter => {
                disable_raw_mode()?;
                println!();
                let command = command.into_string();
                if commands::should_record_history(&command) {
                    history.push(command.clone());
                }
                return Ok(Some(command));
            }
            KeyCode::Up => {
                if let Some(last) = history.len().checked_sub(1) {
                    let index = history_cursor
                        .map(|value: usize| value.saturating_sub(1))
                        .unwrap_or(last);
                    history_cursor = Some(index);
                    command.replace(history[index].clone());
                }
            }
            KeyCode::Down => {
                if let Some(index) = history_cursor {
                    if index + 1 < history.len() {
                        history_cursor = Some(index + 1);
                        command.replace(history[index + 1].clone());
                    } else {
                        history_cursor = None;
                        command.clear();
                    }
                }
            }
            KeyCode::Char('d')
                if key.modifiers == KeyModifiers::CONTROL && command.as_str().is_empty() =>
            {
                disable_raw_mode()?;
                println!();
                return Ok(None);
            }
            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => {
                history_cursor = None;
                command.clear();
            }
            KeyCode::Char('l') if key.modifiers == KeyModifiers::CONTROL => clear = true,
            KeyCode::Tab => {
                let mut prefix = command.prefix().to_owned();
                let notices = complete_command(client, &mut prefix)?;
                command.complete(prefix);
                history_cursor = None;
                if !notices.is_empty() {
                    print!("\r\n{}\r\n", notices.join("\r\n"));
                }
            }
            _ => match command.key(key) {
                Edit::Changed => history_cursor = None,
                Edit::Moved => {}
                Edit::Ignored => continue,
            },
        }
        redraw_command(&mut command, prompt, clear)?;
    }
}

pub(super) struct RawInputGuard;
impl Drop for RawInputGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}
pub(super) fn read_connection_password(
    target: &str,
) -> Result<Option<xfer::credentials::Password>> {
    use std::io::IsTerminal;
    if !io::stdin().is_terminal() {
        anyhow::bail!("interactive password requires a terminal");
    }
    enable_raw_mode()?;
    let _raw = RawInputGuard;
    print!("Password for {}: ", target.escape_default());
    io::stdout().flush()?;
    let mut password = Zeroizing::new(String::new());
    loop {
        match event::read()? {
            Event::Key(key) if key.kind != KeyEventKind::Release => match (key.code, key.modifiers)
            {
                (KeyCode::Enter, _) => {
                    print!("\r\n");
                    io::stdout().flush()?;
                    return Ok(Some(xfer::credentials::Password::new(std::mem::take(
                        &mut *password,
                    ))));
                }
                (KeyCode::Esc, _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                    print!("\r\nconnection cancelled\r\n");
                    io::stdout().flush()?;
                    return Ok(None);
                }
                (KeyCode::Backspace, _) => {
                    password.pop();
                }
                (KeyCode::Char('u'), KeyModifiers::CONTROL) => password.zeroize(),
                (KeyCode::Char(ch), modifiers)
                    if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                        && !ch.is_control()
                        && password.len() + ch.len_utf8() <= 16 * 1024 =>
                {
                    password.push(ch)
                }
                _ => {}
            },
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::rpc_pair;
    use crossterm::event::KeyEvent;
    use std::os::unix::net::UnixStream;

    #[test]
    fn persistent_history_loads_appends_and_ignores_empty_commands() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("nested/history");
        let path = path.to_str().unwrap();
        assert!(history_persist_load(Some(path)).is_empty());
        history_persist_append(Some(path), "  ls  ");
        history_persist_append(Some(path), " \t ");
        history_persist_append(Some(path), "get 日本語.txt");
        assert_eq!(history_persist_load(Some(path)), ["ls", "get 日本語.txt"]);
    }

    #[test]
    fn disabled_history_modes_do_not_load_persistent_commands() {
        for path in [None, Some("session"), Some("/dev/null")] {
            assert!(!history_persist_allowed(path));
            assert!(history_persist_load(path).is_empty());
            history_persist_append(path, "ignored");
        }
    }

    #[test]
    fn clearing_typeahead_discards_buffered_text_before_password_input() {
        let mut input = Typeahead::default();
        input.capture(Event::Paste("discard this buffered input".into()));
        input.clear();
        assert!(input.take().is_empty());
        input.capture(typed(KeyCode::Char('界'), KeyModifiers::NONE));
        assert_eq!(input.take(), "界");
    }

    fn typed(code: KeyCode, modifiers: KeyModifiers) -> Event {
        Event::Key(KeyEvent::new(code, modifiers))
    }

    #[test]
    fn typeahead_buffers_unicode_and_supports_word_and_character_erase() {
        let mut input = Typeahead::default();
        for ch in "show wrong   ".chars() {
            input.capture(typed(KeyCode::Char(ch), KeyModifiers::NONE));
        }
        input.capture(typed(KeyCode::Char('w'), KeyModifiers::CONTROL));
        for ch in "résumé界".chars() {
            input.capture(typed(KeyCode::Char(ch), KeyModifiers::NONE));
        }
        input.capture(typed(KeyCode::Backspace, KeyModifiers::NONE));
        assert_eq!(input.take(), "show résumé");
        assert!(input.take().is_empty());
    }

    #[test]
    fn typeahead_never_queues_enter_or_job_control_keys() {
        let mut input = Typeahead::default();
        for ch in "status".chars() {
            input.capture(typed(KeyCode::Char(ch), KeyModifiers::NONE));
        }
        input.capture(typed(KeyCode::Enter, KeyModifiers::NONE));
        for ch in ['c', 'z', 'd'] {
            input.capture(typed(KeyCode::Char(ch), KeyModifiers::CONTROL));
        }
        input.capture(typed(KeyCode::Up, KeyModifiers::NONE));
        input.capture(Event::Key(KeyEvent {
            kind: KeyEventKind::Release,
            ..KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE)
        }));
        assert_eq!(input.take(), "status");
    }

    #[test]
    fn typeahead_is_bounded_and_paste_cannot_execute_commands() {
        let mut input = Typeahead::default();
        input.capture(Event::Paste("show\nui:auto-pager".into()));
        assert_eq!(input.take(), "show ui:auto-pager");
        input.text = "a".repeat(MAX_TYPEAHEAD_BYTES - 1);
        input.capture(typed(KeyCode::Char('界'), KeyModifiers::NONE));
        assert_eq!(input.text.len(), MAX_TYPEAHEAD_BYTES - 1);
        input.capture(typed(KeyCode::Char('b'), KeyModifiers::NONE));
        input.capture(typed(KeyCode::Char('c'), KeyModifiers::NONE));
        assert_eq!(input.take().len(), MAX_TYPEAHEAD_BYTES);
    }

    #[test]
    fn ctrl_w_erases_whitespace_and_previous_word_safely() {
        for (input, expected) in [
            ("cd photos album", "cd photos "),
            ("cd photos   ", "cd "),
            ("word", ""),
            ("", ""),
            ("   ", ""),
            ("cd\u{3000}résumé", "cd\u{3000}"),
            ("cd résumé/文件", "cd "),
        ] {
            let mut command = input.to_owned();
            erase_previous_word(&mut command);
            assert_eq!(command, expected);
        }
    }

    #[test]
    fn common_prefix_completes_all_matches_on_unicode_boundaries() {
        let choices = |values: &[&str]| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            common_completion_prefix(&choices(&["builddylib-ci/", "builddylib-remote-ci/"])),
            "builddylib-"
        );
        assert_eq!(
            common_completion_prefix(&choices(&["résumé/a", "résumé/b"])),
            "résumé/"
        );
        assert_eq!(common_completion_prefix(&choices(&["é", "ê"])), "");
        assert_eq!(
            common_completion_prefix(&choices(&["cache:ttl", "cache:max-bytes"])),
            "cache:"
        );
        assert_eq!(common_completion_prefix(&choices(&["connect"])), "connect");
        assert_eq!(common_completion_prefix(&[]), "");
    }

    fn complete_with_candidates(command: &str, candidates: &[&str], truncated: bool) -> String {
        let candidates: Vec<String> = candidates.iter().map(|value| value.to_string()).collect();
        let expected = path_completion_context(command).unwrap().2;
        let (mut client, worker) = rpc_pair(1, move |op| {
            let Op::Complete(request) = op else {
                panic!("not a completion")
            };
            assert_eq!(request.kind, expected as i32);
            Reply::Completion(pb::CompletionResult {
                candidates: candidates.clone(),
                truncated,
            })
        });
        let mut command = command.to_owned();
        complete_path_command(&mut client, &mut command).unwrap();
        worker.join().unwrap();
        command
    }

    #[test]
    fn put_completion_uses_local_files_and_preserves_prior_filenames_and_flags() {
        for (input, head, path) in [
            ("put ", "put", ""),
            ("put first.txt pho", "put first.txt", "pho"),
            ("put -- -fi", "put --", "-fi"),
            ("put ~/Downloads/fi", "put", "~/Downloads/fi"),
        ] {
            assert_eq!(
                path_completion_context(input).unwrap(),
                (head.into(), path.into(), pb::CompletionKind::LocalFile)
            );
        }
        assert!(path_completion_context("put -bad file").is_none());
        assert!(path_completion_context("put -bad").is_none());
        assert_eq!(
            shlex::split(&complete_with_candidates(
                "put 'first file' pho",
                &["photo album 日本語.txt"],
                false
            ))
            .unwrap(),
            vec!["put", "first file", "photo album 日本語.txt"]
        );
        assert_eq!(
            shlex::split(&complete_with_candidates(
                "put -- -fi",
                &["-file name"],
                false
            ))
            .unwrap(),
            vec!["put", "--", "-file name"]
        );
        assert_eq!(
            complete_with_candidates("put pho", &["photo1", "photo2"], true),
            "put pho"
        );
    }

    #[test]
    fn lcd_completion_routes_to_local_filesystem_and_quotes_unicode_and_space_names() {
        for (input, path) in [
            ("lcd ", ""),
            ("lcd ~/Do", "~/Do"),
            ("lcd ../pho", "../pho"),
            ("lcd 'photo alb", "photo alb"),
            ("lcd /tmp/pho", "/tmp/pho"),
        ] {
            assert_eq!(
                path_completion_context(input).unwrap(),
                (
                    "lcd".into(),
                    path.into(),
                    pb::CompletionKind::LocalDirectory
                )
            );
        }
        assert!(path_completion_context("lcd two arguments").is_none());
        assert_eq!(
            shlex::split(&complete_with_candidates(
                "lcd 'photo a",
                &["photo album 日本語/"],
                false
            ))
            .unwrap(),
            vec!["lcd", "photo album 日本語/"]
        );
        assert_eq!(
            shlex::split(&complete_with_candidates(
                "lcd ~/Do",
                &["~/Downloads/"],
                false
            ))
            .unwrap(),
            vec!["lcd", "~/Downloads/"]
        );
        assert_eq!(
            complete_with_candidates("lcd pho", &["photo album/", "photos/"], true),
            "lcd pho"
        );
    }

    #[test]
    fn get_completion_preserves_continue_flag_and_prior_filenames() {
        assert_eq!(
            path_completion_context("get -c first.txt pho").unwrap(),
            (
                "get -c first.txt".into(),
                "pho".into(),
                pb::CompletionKind::RemotePath
            )
        );
        assert_eq!(
            path_completion_context("get -c ").unwrap(),
            ("get -c".into(), "".into(), pb::CompletionKind::RemotePath)
        );
        let (head, path, _) = path_completion_context("get 'first file.txt' \"next fi").unwrap();
        assert_eq!(shlex::split(&head).unwrap(), vec!["get", "first file.txt"]);
        assert_eq!(path, "next fi");
        assert_eq!(path_completion_context("get my\\ ").unwrap().1, "my ");
        assert_eq!(path_completion_context("get -- -fi").unwrap().1, "-fi");
        let command = complete_with_candidates("get -c first.txt pho", &["photo album.jpg"], false);
        assert_eq!(
            shlex::split(&command).unwrap(),
            vec!["get", "-c", "first.txt", "photo album.jpg"]
        );
    }

    #[test]
    fn mkdir_completion_preserves_parent_flag_multiple_operands_and_quoted_names() {
        assert_eq!(
            path_completion_context("mkdir -p first second/sub").unwrap(),
            (
                "mkdir -p first".into(),
                "second/sub".into(),
                pb::CompletionKind::RemoteDirectory
            )
        );
        let completed =
            complete_with_candidates("mkdir -p 'first dir' pho", &["photo album/"], false);
        assert_eq!(
            shlex::split(&completed).unwrap(),
            vec!["mkdir", "-p", "first dir", "photo album/"]
        );
        assert!(path_completion_context("mkdir -x path").is_none());
        assert!(path_completion_context("mkdir -p").is_none());
        assert_eq!(path_completion_context("mkdir -- -p").unwrap().1, "-p");
    }

    #[test]
    fn cat_completion_handles_multiple_quoted_paths_and_literal_dash_names() {
        assert_eq!(
            path_completion_context("cat first.txt pho").unwrap(),
            (
                "cat first.txt".into(),
                "pho".into(),
                pb::CompletionKind::RemotePath
            )
        );
        let completed = complete_with_candidates("cat 'first file' -- -fi", &["-file name"], false);
        assert_eq!(
            shlex::split(&completed).unwrap(),
            vec!["cat", "first file", "--", "-file name"]
        );
    }

    #[test]
    fn ls_tab_handles_files_directories_and_quoted_nested_paths() {
        assert_eq!(
            complete_with_candidates("ls pho", &["photos.txt", "photos/"], false),
            "ls photos"
        );
        assert_eq!(
            complete_with_candidates("ls README", &["README.txt"], false),
            "ls README.txt"
        );
        assert_eq!(
            complete_with_candidates("ls ar", &["archive/"], false),
            "ls archive/"
        );
        let command = complete_with_candidates("ls folder/my", &["folder/my file.txt"], false);
        assert_eq!(
            shlex::split(&command).unwrap(),
            vec!["ls", "folder/my file.txt"]
        );
    }

    #[test]
    fn ls_completion_preserves_flags_and_the_end_of_options_marker() {
        for prefix in [
            "ls -laF",
            "ls -l -a -S -r",
            "ls --no-long --no-classify",
            "ls --",
        ] {
            let command = format!("{prefix} pho");
            assert_eq!(
                path_completion_context(&command).unwrap(),
                (prefix.into(), "pho".into(), pb::CompletionKind::RemotePath)
            );
            let completed = complete_with_candidates(&command, &["photo album/"], false);
            let mut expected = shlex::split(prefix).unwrap();
            expected.push("photo album/".into());
            assert_eq!(shlex::split(&completed).unwrap(), expected);
        }
        assert_eq!(path_completion_context("ls -l ").unwrap().1, "");
        assert!(path_completion_context("ls -z pho").is_none());
        assert!(path_completion_context("ls -l").is_none());
        assert_eq!(path_completion_context("ls -- -l").unwrap().1, "-l");
        assert!(path_completion_context("ls first second").is_none());
        assert!(path_completion_context("ls -- first second").is_none());
    }

    #[test]
    fn cd_tab_extends_to_shared_prefix_before_quoting() {
        assert_eq!(
            complete_with_candidates(
                "cd bui",
                &["builddylib-ci/", "builddylib-remote-ci/"],
                false
            ),
            "cd builddylib-"
        );
        let command =
            complete_with_candidates("cd 'photo a", &["photo album/", "photo archive/"], false);
        assert_eq!(command, "cd 'photo a"); // Already at the shared point; keep unfinished input intact.
        let command =
            complete_with_candidates("cd 'photo ", &["photo album-a/", "photo album-b/"], false);
        assert_eq!(shlex::split(&command).unwrap(), vec!["cd", "photo album-"]);
        // A limited cache does not contain ALL matches: do not claim a common
        // prefix that might exclude unseen candidates.
        assert_eq!(
            complete_with_candidates("cd bui", &["builddylib-ci/", "builddylib-remote-ci/"], true),
            "cd bui"
        );
    }

    #[test]
    fn directory_completion_handles_quoted_and_unfinished_arguments() {
        for (input, expected) in [
            ("cd ", ""),
            ("cd ../pho", "../pho"),
            ("cd 'photo alb", "photo alb"),
            ("cd \"photo alb", "photo alb"),
        ] {
            assert_eq!(path_completion_context(input).unwrap().1, expected);
        }
        assert!(path_completion_context("cd two arguments").is_none());
    }

    #[test]
    fn cd_completion_uses_daemon_rpc_and_quotes_paths_with_spaces() {
        let result = complete_with_candidates("cd pho", &["photo album/"], false);
        assert_eq!(shlex::split(&result).unwrap(), vec!["cd", "photo album/"]);
    }

    #[test]
    fn initial_command_expansion_appends_a_space_only_for_unique_matches() {
        let (stream, _server) = UnixStream::pair().unwrap();
        let mut client = Connection::new(stream);
        for (before, expected) in [
            ("lc", "lcd "),
            ("l", "l"),
            ("ls", "ls "),
            ("ge", "get "),
            ("", ""),
        ] {
            let mut prefix = before.to_owned();
            complete_command(&mut client, &mut prefix).unwrap();
            assert_eq!(prefix, expected);
        }
    }

    #[test]
    fn completion_at_the_caret_preserves_later_arguments_and_uses_the_renamed_setting() {
        for (text, before, kind, candidate, expected) in [
            (
                "put 'photo old.txt' later.txt",
                "put 'pho",
                Some(pb::CompletionKind::LocalFile),
                "photo album.txt",
                "put 'photo album.txt' later.txt",
            ),
            (
                "lcd 'photo old'",
                "lcd 'pho",
                Some(pb::CompletionKind::LocalDirectory),
                "photo album/",
                "lcd 'photo album/'",
            ),
            (
                "get -c 'photo album' second.txt",
                "get -c 'pho",
                Some(pb::CompletionKind::RemotePath),
                "photo album/",
                "get -c 'photo album/' second.txt",
            ),
            (
                "set ui:ls-default-opt '-lF'",
                "set ui:ls-default-opt",
                None,
                "ui:ls-default-options",
                "set ui:ls-default-options '-lF'",
            ),
        ] {
            let (mut client, worker) = rpc_pair(1, move |op| {
                if let Some(kind) = kind {
                    assert_eq!(
                        op,
                        Op::Complete(pb::CompletionRequest {
                            kind: kind as i32,
                            input: "pho".into()
                        })
                    );
                    Reply::Completion(pb::CompletionResult {
                        candidates: vec![candidate.into()],
                        truncated: false,
                    })
                } else {
                    assert!(matches!(op, Op::Settings(_)));
                    Reply::Settings(pb::SettingsResult {
                        settings: vec![pb::Setting {
                            name: candidate.into(),
                            description: "options".into(),
                            value: Some(pb::setting::Value::Text("-lF".into())),
                        }],
                    })
                }
            });
            let mut command = CommandLine::new(text.into());
            command.key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
            for _ in before.chars() {
                command.key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
            }
            assert_eq!(command.prefix(), before);
            let mut prefix = command.prefix().to_owned();
            complete_command(&mut client, &mut prefix).unwrap();
            command.complete(prefix);
            assert_eq!(command.as_str(), expected);
            worker.join().unwrap();
        }
    }

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
