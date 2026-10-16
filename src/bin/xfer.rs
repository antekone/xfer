#[path = "xfer/line_editor.rs"]
mod line_editor;
use line_editor::{CommandLine, Edit};

use std::{
    fs, io,
    io::{BufRead, BufReader, Write},
    os::unix::{net::UnixStream, process::CommandExt},
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute, queue,
    terminal::{
        disable_raw_mode, enable_raw_mode, BeginSynchronizedUpdate, Clear, ClearType,
        EndSynchronizedUpdate, EnterAlternateScreen, LeaveAlternateScreen,
    },
};
use zeroize::{Zeroize, Zeroizing};

use xfer::commands::{
    self, ClientApp, CommandContext, CommandResult, Completion, ExecutionSite, ParserContext,
};

fn load_history(path: Option<&str>) -> Vec<String> {
    match path {
        Some(path) if path != "session" && path != "/dev/null" => fs::read_to_string(path)
            .map(|data| data.lines().map(str::to_owned).collect())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn append_history(path: Option<&str>, command: &str) {
    let Some(path) = path.filter(|path| *path != "session" && *path != "/dev/null") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| writeln!(file, "{command}"));
}

fn daemon_ready(instance: &xfer::Instance) -> bool {
    let Ok(mut stream) = UnixStream::connect(&instance.socket) else {
        return false;
    };
    if stream.write_all(b"PROBE\n").is_err() {
        return false;
    }
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).is_ok() && line.trim() == "READY"
}

fn erase_previous_word(command: &mut String) {
    command.truncate(line_editor::previous_word_start(command));
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

fn path_completion_context(command: &str) -> Option<(String, String, String)> {
    let command = command.trim_start();
    let verb = command.split_whitespace().next()?;
    let Completion::RemotePath { request, .. } = commands::completion_for(verb) else {
        return None;
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
    Some((head, path, request.to_owned()))
}

fn complete_path_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    command: &mut String,
) -> Result<Vec<String>> {
    let Some((head, path, method)) = path_completion_context(command) else {
        return Ok(Vec::new());
    };
    stream.write_all(format!("{method} {}\n", serde_json::to_string(&path)?).as_bytes())?;
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 {
        anyhow::bail!("daemon disconnected");
    }
    let mut notices = Vec::new();
    let mut choices = Vec::new();
    let mut complete_set = true;
    if let Some(json) = response.strip_prefix("COMPLETION ") {
        let result: xfer::cache::PathCompletion = serde_json::from_str(json)?;
        for path in result.candidates {
            // Terminal control characters cannot be safely inserted by this editor.
            if !path.chars().any(char::is_control) {
                choices.push(path);
            }
        }
        if result.truncated {
            complete_set = false;
            notices.push("completion listing limited to cached entries".to_owned());
        }
    } else if let Some(json) = response.strip_prefix("COMPLETION_ERROR ") {
        let error: String = serde_json::from_str(json)?;
        notices.push(
            error
                .chars()
                .map(|ch| if ch.is_control() { ' ' } else { ch })
                .collect(),
        );
    } else {
        anyhow::bail!("invalid completion response");
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
fn complete_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    command: &mut String,
) -> Result<Vec<String>> {
    let verb = command.split_whitespace().next().unwrap_or("");
    let completion = commands::completion_for(verb);
    if matches!(completion, Completion::RemotePath { .. }) && command.contains(char::is_whitespace)
    {
        return complete_path_command(stream, reader, command);
    }
    let matches = if !command.contains(char::is_whitespace) {
        commands::visible_names()
            .filter(|name| name.starts_with(command.as_str()))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    } else if completion == Completion::Settings {
        stream.write_all(b"COMMAND show\n")?;
        let mut choices = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                anyhow::bail!("daemon disconnected");
            }
            if line.trim() == "DONE" {
                break;
            }
            if let Some(name) = line
                .strip_prefix("OUTPUT ")
                .and_then(|v| v.split_once(": ").map(|(name, _)| name))
            {
                choices.push(name.to_owned());
            }
        }
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
        } else if common.len() > command.len() {
            *command = common.to_owned();
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
struct Typeahead {
    text: String,
}
impl Typeahead {
    fn push(&mut self, ch: char) {
        if !ch.is_control() && self.text.len() + ch.len_utf8() <= MAX_TYPEAHEAD_BYTES {
            self.text.push(ch);
        }
    }
    fn capture(&mut self, event: Event) {
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

fn read_attached_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
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
                if !command.trim().is_empty() {
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
                let notices = complete_command(stream, reader, &mut prefix)?;
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

struct LocalOutput(std::path::PathBuf);
impl Drop for LocalOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
struct RawInputGuard;
impl Drop for RawInputGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}
struct PagerTerminal;
impl Drop for PagerTerminal {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            EndSynchronizedUpdate,
            LeaveAlternateScreen,
            cursor::Show
        );
        let _ = disable_raw_mode();
    }
}

struct CopiedOutput {
    bytes: u64,
    rows: usize,
    last: Option<u8>,
}

/// Binary-safe bounded IPC copy, shared by paged output and raw cat stdout.
fn copy_job_output(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
    output: &mut dyn Write,
) -> Result<CopiedOutput> {
    let mut copied = CopiedOutput {
        bytes: 0,
        rows: 0,
        last: None,
    };
    loop {
        stream.write_all(format!("COMMAND read-output {id} {}\n", copied.bytes).as_bytes())?;
        let mut response = String::new();
        if reader.read_line(&mut response)? == 0 {
            anyhow::bail!("daemon disconnected");
        }
        let chunk = response
            .strip_prefix("OUTPUT CHUNK ")
            .ok_or_else(|| anyhow::anyhow!("invalid output response"))?;
        let (next, bytes) = chunk
            .trim()
            .split_once(' ')
            .ok_or_else(|| anyhow::anyhow!("invalid output chunk"))?;
        let next: u64 = next.parse()?;
        let bytes: Vec<u8> = serde_json::from_str(bytes)?;
        response.clear();
        if reader.read_line(&mut response)? == 0 || response.trim() != "DONE" {
            anyhow::bail!("incomplete output response");
        }
        if bytes.len() > 64 * 1024 || copied.bytes.checked_add(bytes.len() as u64) != Some(next) {
            anyhow::bail!("invalid output offset or chunk size");
        }
        if bytes.is_empty() {
            break;
        }
        output.write_all(&bytes)?;
        copied.bytes = next;
        copied.rows += bytes.iter().filter(|&&byte| byte == b'\n').count();
        copied.last = bytes.last().copied();
    }
    output.flush()?;
    Ok(copied)
}

fn display_job_output(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
) -> Result<bool> {
    use std::io::{Seek, SeekFrom};
    // Keep client memory bounded too: receive at most 64 KiB at a time.
    let path = std::env::temp_dir().join(format!(
        "xfer-output-{}-{id}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    let _cleanup = LocalOutput(path);
    let copied = copy_job_output(stream, reader, id, &mut file)?;
    stream.write_all(b"COMMAND show ui:auto-pager\n")?;
    let mut response = String::new();
    reader.read_line(&mut response)?;
    let auto_pager = response.starts_with("OUTPUT ui:auto-pager: true");
    response.clear();
    reader.read_line(&mut response)?;
    file.seek(SeekFrom::Start(0))?;
    let (_, height) = crossterm::terminal::size().unwrap_or((80, 24));
    if !auto_pager || copied.rows < height.saturating_sub(1) as usize {
        std::io::copy(&mut file, &mut io::stdout())?;
        io::stdout().flush()?;
        return Ok(copied.bytes > 0);
    }
    page_output_file(&mut file)?;
    Ok(copied.bytes > 0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PagerAction {
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
    Visual,
    Quit,
}

fn pager_action(key: KeyEvent) -> Option<PagerAction> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    let plain = !key
        .modifiers
        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
    Some(match key.code {
        KeyCode::Esc => PagerAction::Quit,
        KeyCode::Char('q') if plain => PagerAction::Quit,
        KeyCode::Up => PagerAction::Up,
        KeyCode::Down => PagerAction::Down,
        KeyCode::Char('k') if plain => PagerAction::Up,
        KeyCode::Char('j') if plain => PagerAction::Down,
        KeyCode::PageUp => PagerAction::PageUp,
        KeyCode::PageDown => PagerAction::PageDown,
        KeyCode::Char(' ') if plain => PagerAction::PageDown,
        KeyCode::Char('f') if key.modifiers == KeyModifiers::CONTROL => PagerAction::PageDown,
        KeyCode::Char('b') if key.modifiers == KeyModifiers::CONTROL => PagerAction::PageUp,
        KeyCode::Home => PagerAction::Home,
        KeyCode::End => PagerAction::End,
        KeyCode::Char('|') if plain => PagerAction::Visual,
        _ => return None,
    })
}

fn pager_page_size(height: u16) -> usize {
    usize::from(height.saturating_sub(1)).max(1)
}

fn pager_offset(action: PagerAction, offset: usize, rows: usize, page_size: usize) -> usize {
    let page_size = page_size.max(1);
    let last = rows.saturating_sub(page_size);
    let offset = offset.min(last);
    match action {
        PagerAction::Up => offset.saturating_sub(1),
        PagerAction::Down => offset.saturating_add(1).min(last),
        PagerAction::PageUp => offset.saturating_sub(page_size),
        PagerAction::PageDown => offset.saturating_add(page_size).min(last),
        PagerAction::Home => 0,
        PagerAction::End => last,
        _ => offset,
    }
}

fn pager_label(offset: usize, rows: usize) -> String {
    format!(
        "-- {}/{}  j/k ↑↓  ^F/^B PgDn/PgUp  Home/End  | VISUAL  q quit --",
        (offset + 1).min(rows),
        rows
    )
}

/// Line offsets live on disk, not in a growing in-memory list. This allows
/// single-line navigation and End on arbitrarily large spooled output.
struct PagerIndex {
    offsets: fs::File,
    rows: usize,
}
impl PagerIndex {
    fn new(source: &mut fs::File) -> io::Result<Self> {
        use std::io::{Read, Seek, SeekFrom};
        let length = source.metadata()?.len();
        let mut offsets = io::BufWriter::new(tempfile::tempfile()?);
        let mut rows = 0;
        if length > 0 {
            offsets.write_all(&0u64.to_le_bytes())?;
            rows += 1;
        }
        source.seek(SeekFrom::Start(0))?;
        let mut position = 0;
        let mut buffer = [0; 64 * 1024];
        loop {
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            for &byte in &buffer[..count] {
                position += 1;
                if byte == b'\n' && position < length {
                    offsets.write_all(&position.to_le_bytes())?;
                    rows += 1;
                }
            }
        }
        offsets.flush()?;
        Ok(Self {
            offsets: offsets.into_inner().map_err(|e| e.into_error())?,
            rows,
        })
    }
    fn offset(&mut self, row: usize) -> io::Result<u64> {
        use std::io::{Read, Seek, SeekFrom};
        if self.rows == 0 {
            return Ok(0);
        }
        self.offsets
            .seek(SeekFrom::Start(row.min(self.rows - 1) as u64 * 8))?;
        let mut bytes = [0; 8];
        self.offsets.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
}

fn visual_command(editor: &str, path: &std::path::Path) -> Result<Command> {
    let args = shlex::split(editor)
        .filter(|args| !args.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid VISUAL/EDITOR command"))?;
    let mut command = Command::new(&args[0]);
    command.args(&args[1..]).arg(path);
    Ok(command)
}

fn pager_output_copy(
    write_output: impl FnOnce(&mut fs::File) -> io::Result<()>,
) -> io::Result<tempfile::NamedTempFile> {
    use std::os::unix::fs::PermissionsExt;
    let mut copy = tempfile::Builder::new()
        .prefix("xfer-pager-")
        .suffix(".txt")
        .permissions(fs::Permissions::from_mode(0o600))
        .tempfile()?;
    write_output(copy.as_file_mut())?;
    copy.as_file_mut().flush()?;
    Ok(copy)
}

/// Always restore the pager's terminal, including on editor launch/exit errors.
struct ResumePager;
impl Drop for ResumePager {
    fn drop(&mut self) {
        let _ = enable_raw_mode();
        let _ = execute!(io::stdout(), EnterAlternateScreen, cursor::Hide);
    }
}

fn open_pager_visual(write_output: impl FnOnce(&mut fs::File) -> io::Result<()>) -> Result<()> {
    let copy = pager_output_copy(write_output)?;
    let editor = std::env::var("VISUAL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "vi".into());
    let mut command = visual_command(&editor, copy.path())?;
    // An interactive editor needs the terminal on stdin, not a pipe of output.
    // Pass a private copy as a file argument so edits cannot alter pager data.
    disable_raw_mode()?;
    let _resume = ResumePager;
    execute!(io::stdout(), LeaveAlternateScreen, cursor::Show)?;
    let status = command.status()?;
    if !status.success() {
        anyhow::bail!("editor exited with {status}");
    }
    Ok(())
}

/// Compare one screen at a time and send changed rows in a single synchronized
/// write. Never blank the screen before painting; unsupported terminals can
/// safely ignore synchronized-update escapes and still avoid clear/repaint flicker.
#[derive(Default)]
struct PagerDisplay {
    size: Option<(u16, u16)>,
    rows: Vec<String>,
}
impl PagerDisplay {
    fn invalidate(&mut self) {
        self.size = None;
    }

    fn update(
        &mut self,
        width: u16,
        height: u16,
        lines: &[String],
        label: &str,
    ) -> io::Result<Vec<u8>> {
        let mut rows = vec![String::new(); usize::from(height)];
        for (row, line) in rows
            .iter_mut()
            .take(usize::from(height.saturating_sub(1)))
            .zip(lines)
        {
            *row = fit_status_line(line, width);
        }
        if let Some(row) = rows.last_mut() {
            *row = fit_status_line(label, width);
        }
        let resized = self.size != Some((width, height));
        let mut output = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            if !resized && self.rows.get(index) == Some(row) {
                continue;
            }
            if output.is_empty() {
                queue!(output, BeginSynchronizedUpdate)?;
            }
            queue!(output, cursor::MoveTo(0, index as u16))?;
            output.extend_from_slice(row.as_bytes());
            // Erase only the suffix left by an older, longer line.
            queue!(output, Clear(ClearType::UntilNewLine))?;
        }
        if !output.is_empty() {
            queue!(output, EndSynchronizedUpdate)?;
        }
        self.size = Some((width, height));
        self.rows = rows;
        Ok(output)
    }

    fn draw(
        &mut self,
        out: &mut impl Write,
        width: u16,
        height: u16,
        lines: &[String],
        label: &str,
    ) -> io::Result<()> {
        let frame = self.update(width, height, lines, label)?;
        if !frame.is_empty() {
            out.write_all(&frame)?;
            out.flush()?;
        }
        Ok(())
    }
}

/// Leave pager mode, then append its final frame to the normal terminal buffer.
/// This preserves earlier commands in scrollback while leaving the last page visible.
fn write_pager_frame(out: &mut impl Write, rows: &[String]) -> io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    out.write_all(b"\r\n")?;
    for (index, row) in rows.iter().enumerate() {
        out.write_all(row.as_bytes())?;
        if index + 1 != rows.len() {
            out.write_all(b"\r\n")?;
        }
    }
    out.flush()
}

fn page_output_file(file: &mut fs::File) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut index = PagerIndex::new(file)?;
    enable_raw_mode()?;
    let terminal = PagerTerminal;
    execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
    let mut offset = 0;
    let mut notice = None;
    let mut display = PagerDisplay::default();
    loop {
        let (width, height) = crossterm::terminal::size().unwrap_or((80, 24));
        let page_size = pager_page_size(height);
        offset = offset.min(index.rows.saturating_sub(page_size));
        let mut input = BufReader::new(&mut *file);
        input.seek(SeekFrom::Start(index.offset(offset)?))?;
        let mut visible = Vec::with_capacity(page_size);
        for _ in 0..page_size {
            let mut line = String::new();
            if input.read_line(&mut line)? == 0 {
                break;
            }
            visible.push(line.trim_end_matches(['\r', '\n']).to_owned());
        }
        let label = notice
            .take()
            .unwrap_or_else(|| pager_label(offset, index.rows));
        display.draw(&mut io::stdout(), width, height, &visible, &label)?;
        if let Event::Key(key) = event::read()? {
            if let Some(action) = pager_action(key) {
                match action {
                    PagerAction::Quit => break,
                    PagerAction::Visual => {
                        notice = open_pager_visual(|copy| {
                            file.seek(SeekFrom::Start(0))?;
                            io::copy(file, copy).map(|_| ())
                        })
                        .err()
                        .map(|error| error.to_string());
                        display.invalidate();
                    }
                    action => offset = pager_offset(action, offset, index.rows, page_size),
                }
            }
        }
    }
    // Restore the normal buffer before emitting output, otherwise the frame
    // disappears with the alternate screen.
    drop(terminal);
    write_pager_frame(&mut io::stdout(), &display.rows)?;
    Ok(())
}

/// Never let an in-place status wrap: clearing only the current line cannot
/// erase wrapped rows. Full errors are emitted once as ordinary scrollback.
fn fit_status_line(text: &str, terminal_width: u16) -> String {
    use unicode_width::UnicodeWidthChar;
    let limit = terminal_width.saturating_sub(1) as usize;
    let clean = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    if clean
        .chars()
        .map(|ch| ch.width().unwrap_or(0))
        .sum::<usize>()
        <= limit
    {
        return clean;
    }
    if limit == 0 {
        return String::new();
    }
    let mut used = 0;
    let mut clipped = String::new();
    for ch in clean.chars() {
        let width = ch.width().unwrap_or(0);
        if used + width > limit - 1 {
            break;
        }
        clipped.push(ch);
        used += width;
    }
    clipped.push('…');
    clipped
}

#[derive(Default)]
struct ForegroundDisplay {
    previous_status: Option<String>,
    previous_line: Option<String>,
}
impl ForegroundDisplay {
    fn update(&mut self, status: &str, terminal_width: u16) -> String {
        let retry = status
            .split_once(" failed: ")
            .and_then(|(attempt, message)| {
                message
                    .rsplit_once("; retrying in ")
                    .map(|(_, delay)| format!("{attempt}; retrying in {delay}"))
            });
        let mut output = String::new();
        if self.previous_status.as_deref() != Some(status) && retry.is_some() {
            output.push_str(&format!("\r\x1b[2K{status}\r\n"));
            self.previous_line = None;
        }
        let line = fit_status_line(
            &format!(
                "{}  (^C cancel, ^Z background)",
                retry.as_deref().unwrap_or(status)
            ),
            terminal_width,
        );
        if self.previous_line.as_deref() != Some(&line) {
            output.push_str(&format!("\r\x1b[2K{line}"));
            self.previous_line = Some(line);
        }
        self.previous_status = Some(status.to_owned());
        output
    }
}

#[derive(Default)]
struct TransferRate {
    samples: std::collections::VecDeque<(Instant, u64)>,
}
impl TransferRate {
    fn sample(&mut self, stats: &xfer::transfer::TransferProgress) -> f64 {
        let now = Instant::now();
        self.samples.push_back((now, stats.transferred));
        while self.samples.len() > 1
            && now.duration_since(self.samples.front().unwrap().0) > Duration::from_secs(2)
        {
            self.samples.pop_front();
        }
        while self.samples.len() > 32 {
            self.samples.pop_front();
        }
        let (time, bytes) = self.samples.front().unwrap();
        let elapsed = now.duration_since(*time).as_secs_f64();
        if elapsed >= 0.1 {
            stats.transferred.saturating_sub(*bytes) as f64 / elapsed
        } else {
            stats.transferred as f64 / (stats.elapsed_ms as f64 / 1000.0).max(0.001)
        }
    }
}
fn transfer_bytes(bytes: f64) -> String {
    let mut value = bytes.max(0.0);
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut unit = 0;
    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0}B")
    } else {
        format!("{value:.1}{}", units[unit])
    }
}
fn transfer_time(seconds: u64) -> String {
    if seconds >= 3600 {
        format!(
            "{}:{:02}:{:02}",
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60
        )
    } else {
        format!("{:02}:{:02}", seconds / 60, seconds % 60)
    }
}
fn render_transfer(
    id: u64,
    stats: &xfer::transfer::TransferProgress,
    speed: f64,
    width: u16,
) -> String {
    let ratio = if stats.total == 0 {
        1.0
    } else {
        (stats.done as f64 / stats.total as f64).clamp(0.0, 1.0)
    };
    let length = if width >= 75 { 10 } else { 5 };
    let filled = (ratio * length as f64) as usize;
    let bar = format!("{}{}", "█".repeat(filled), "░".repeat(length - filled));
    let eta = if stats.done >= stats.total {
        "00:00".into()
    } else if speed > 0.0 {
        transfer_time(((stats.total - stats.done) as f64 / speed).ceil() as u64)
    } else {
        "--:--".into()
    };
    let metrics = format!(
        " [{bar}] {:3.0}% {}/{} {}/s t{} ETA{}",
        (ratio * 100.0).floor(),
        transfer_bytes(stats.done as f64),
        transfer_bytes(stats.total as f64),
        transfer_bytes(speed),
        transfer_time(stats.elapsed_ms / 1000),
        eta
    );
    let prefix = format!("[{id}] ");
    let label = if stats.file_count > 1 {
        format!("{}/{} {}", stats.file_index, stats.file_count, stats.file)
    } else {
        stats.file.clone()
    };
    let budget = width.saturating_sub(
        unicode_width::UnicodeWidthStr::width(metrics.as_str()) as u16 + prefix.len() as u16 + 1,
    );
    let label = fit_status_line(&label, budget.saturating_add(1));
    fit_status_line(&format!("{prefix}{label}{metrics}"), width)
}

fn foreground_job(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
    typeahead: &mut Typeahead,
) -> Result<()> {
    enable_raw_mode()?;
    let _raw_input = RawInputGuard;
    let mut display = ForegroundDisplay::default();
    let mut transfer_rate = TransferRate::default();
    loop {
        stream.write_all(format!("COMMAND status {id}\n").as_bytes())?;
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            anyhow::bail!("daemon disconnected");
        }
        let status = line
            .strip_prefix("OUTPUT ")
            .unwrap_or(&line)
            .trim()
            .to_owned();
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            anyhow::bail!("daemon disconnected");
        }
        let state = status.split_whitespace().nth(1);
        let raw_output = status.split_whitespace().nth(2) == Some("RAW");
        let human_status = if raw_output {
            status.replacen(" RAW", "", 1)
        } else {
            status.clone()
        };
        if matches!(state, Some("Completed" | "Cancelled" | "Failed")) || status == "no such job" {
            print!("\r\x1b[2K");
            io::stdout().flush()?;
            disable_raw_mode()?;
            if status == "no such job" {
                println!("{status}");
            } else {
                // Show the result once, not both as terminal status and spool text.
                if raw_output {
                    let copied = copy_job_output(stream, reader, id, &mut io::stdout())?;
                    // This is a terminal prompt separator, not part of the file:
                    // avoid erasing an unterminated last line on prompt redraw.
                    use std::io::IsTerminal;
                    if io::stdout().is_terminal() && copied.last.is_some_and(|byte| byte != b'\n') {
                        println!();
                    }
                    if state != Some("Completed") {
                        eprintln!("{human_status}");
                    }
                } else if !display_job_output(stream, reader, id)? && state != Some("Completed") {
                    println!("{status}");
                }
                stream.write_all(format!("COMMAND ack-output {id}\n").as_bytes())?;
                loop {
                    line.clear();
                    if reader.read_line(&mut line)? == 0 {
                        anyhow::bail!("daemon disconnected");
                    }
                    if line.trim() == "DONE" {
                        break;
                    }
                    let response = line.strip_prefix("OUTPUT ").unwrap_or(&line).trim();
                    if response != "output acknowledged" {
                        eprintln!("{response}");
                    }
                }
            }
            return Ok(());
        }
        let width = crossterm::terminal::size()
            .map(|(width, _)| width)
            .ok()
            .filter(|width| *width > 0)
            .unwrap_or(80);
        let transfer_status = if let Some((_, json)) = status.split_once(" TRANSFER ") {
            serde_json::from_str::<xfer::transfer::TransferProgress>(json)
                .ok()
                .map(|stats| render_transfer(id, &stats, transfer_rate.sample(&stats), width))
        } else {
            None
        };
        let redraw = if let Some(line) = transfer_status {
            if display.previous_line.as_deref() == Some(&line) {
                String::new()
            } else {
                display.previous_line = Some(line.clone());
                format!("\r\x1b[2K{line}")
            }
        } else {
            display.update(&human_status, width)
        };
        if !redraw.is_empty() {
            print!("{redraw}");
            io::stdout().flush()?;
        }
        if event::poll(Duration::from_millis(150))? {
            let event = event::read()?;
            if let Event::Key(key) = &event {
                if key.kind != KeyEventKind::Release {
                    match (key.code, key.modifiers) {
                        (KeyCode::Char('z'), KeyModifiers::CONTROL) => {
                            disable_raw_mode()?;
                            println!("\r\njob {id} moved to background");
                            return Ok(());
                        }
                        (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                            stream.write_all(format!("COMMAND cancel {id}\n").as_bytes())?;
                            loop {
                                line.clear();
                                if reader.read_line(&mut line)? == 0 {
                                    anyhow::bail!("daemon disconnected");
                                }
                                if line.trim() == "DONE" {
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            typeahead.capture(event);
        }
    }
}

fn read_connection_password(target: &str) -> Result<Option<xfer::credentials::Password>> {
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

fn send_connection_password(
    stream: &mut UnixStream,
    target: String,
    password: xfer::credentials::Password,
) -> Result<()> {
    let credentials = xfer::credentials::PasswordConnect { target, password };
    let json = Zeroizing::new(serde_json::to_string(&credentials)?);
    stream.write_all(b"CONNECT_PASSWORD ")?;
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    Ok(())
}

struct AttachedCommandContext<'a> {
    stream: &'a mut UnixStream,
    reader: &'a mut BufReader<UnixStream>,
    typeahead: &'a mut Typeahead,
}
impl ClientApp for AttachedCommandContext<'_> {
    fn detach(&mut self) -> Result<()> {
        self.stream.write_all(b"DETACH\n")?;
        Ok(())
    }
    fn foreground(&mut self, id: u64) -> Result<()> {
        foreground_job(self.stream, self.reader, id, self.typeahead)
    }
}

/// Shared execution path for rc lines and interactive commands.
fn dispatch_attached_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    command: &str,
    history_file: Option<&str>,
    typeahead: &mut Typeahead,
) -> Result<bool> {
    if command.trim().is_empty() {
        return Ok(true);
    }
    let parser = match ParserContext::parse(command) {
        Ok(parser) => parser,
        Err(error) => {
            append_history(history_file, command);
            eprintln!("{error}");
            return Ok(true);
        }
    };
    let definition = commands::find(&parser);
    if definition.is_none_or(|command| command.record_history()) {
        append_history(history_file, command);
    }
    if let Some(definition) = definition.filter(|command| command.site() == ExecutionSite::Client) {
        let mut app = AttachedCommandContext {
            stream,
            reader,
            typeahead,
        };
        match definition.execute(&mut CommandContext::Client { app: &mut app }, &parser) {
            Ok(CommandResult::Detach) => return Ok(false),
            Ok(CommandResult::Output(lines)) => {
                for line in lines {
                    println!("{line}");
                }
            }
            Err(error) => eprintln!("{error:#}"),
        }
        return Ok(true);
    }
    let mut line = String::new();
    stream.write_all(format!("COMMAND {command}\n").as_bytes())?;
    let mut foreground = None;
    let mut edit_bookmarks = None;
    let mut password_sent = false;
    let mut password_target: Option<String> = None;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            anyhow::bail!("daemon disconnected");
        }
        if line.trim() == "DONE" {
            if let Some(target) = password_target.take() {
                if password_sent {
                    anyhow::bail!("daemon requested a password twice");
                }
                typeahead.text.zeroize();
                let Some(password) = read_connection_password(&target)? else {
                    return Ok(true);
                };
                send_connection_password(stream, target, password)?;
                password_sent = true;
                continue;
            }
            break;
        }
        if let Some(target) = line.strip_prefix("OUTPUT PASSWORD_REQUIRED ") {
            password_target = Some(serde_json::from_str::<String>(target)?);
            continue;
        }
        if let Some(id) = line
            .strip_prefix("OUTPUT JOB ")
            .and_then(|value| value.trim().parse().ok())
        {
            foreground = Some(id);
            continue; // JOB is IPC control information, not user-visible output.
        }
        if let Some(path) = line.strip_prefix("OUTPUT BOOKMARK_EDIT ") {
            edit_bookmarks = Some(path.trim_end_matches(['\r', '\n']).to_owned());
        } else {
            print!("{}", line.strip_prefix("OUTPUT ").unwrap_or(&line));
        }
    }
    if let Some(path) = edit_bookmarks {
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .unwrap_or_else(|_| "vi".into());
        match shlex::split(&editor).filter(|args| !args.is_empty()) {
            Some(args) => match Command::new(&args[0]).args(&args[1..]).arg(&path).status() {
                Ok(status) if status.success() => {
                    stream.write_all(b"COMMAND bookmark reload\n")?;
                    loop {
                        line.clear();
                        if reader.read_line(&mut line)? == 0 {
                            anyhow::bail!("daemon disconnected");
                        }
                        if line.trim() == "DONE" {
                            break;
                        }
                        print!("{}", line.strip_prefix("OUTPUT ").unwrap_or(&line));
                    }
                }
                Ok(status) => eprintln!("editor exited with {status}"),
                Err(error) => eprintln!("cannot run editor: {error}"),
            },
            None => eprintln!("invalid VISUAL/EDITOR command"),
        }
    }
    if let Some(id) = foreground {
        foreground_job(stream, reader, id, typeahead)?;
    }
    Ok(true)
}

fn read_startup_commands(path: &std::path::Path) -> Result<Vec<String>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(anyhow::anyhow!("read {}: {error}", path.display())),
    };
    Ok(text
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .map(str::to_owned)
        .collect())
}

fn report_startup_file(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    path: &std::path::Path,
) -> Result<()> {
    stream.write_all(format!("STARTUP_FILE {}\n", serde_json::to_string(path)?).as_bytes())?;
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 || response.trim() != "STARTUP_FILE_OK" {
        anyhow::bail!("cannot report startup file: {}", response.trim());
    }
    Ok(())
}

fn session_prompt(info: &xfer::core::SessionInfo) -> String {
    let safe = |value: &str| {
        value
            .chars()
            .flat_map(|ch| {
                if ch.is_control() {
                    ch.escape_default().collect::<Vec<_>>()
                } else {
                    vec![ch]
                }
            })
            .collect::<String>()
    };
    match (&info.protocol, &info.cwd) {
        (Some(protocol), Some(cwd)) => format!("xfer[{}:{}]> ", safe(protocol), safe(cwd)),
        (Some(protocol), None) => format!("xfer[{}]> ", safe(protocol)),
        _ => "xfer> ".into(),
    }
}

fn fetch_session_prompt(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
) -> Result<String> {
    stream.write_all(b"SESSION\n")?;
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 {
        anyhow::bail!("daemon disconnected");
    }
    let json = response
        .strip_prefix("SESSION ")
        .ok_or_else(|| anyhow::anyhow!("invalid session response"))?;
    Ok(session_prompt(&serde_json::from_str(json)?))
}

fn attach(args: &[String]) -> Result<()> {
    let all = xfer::instances()?;
    let selected = match args.get(2) {
        Some(id) => all
            .into_iter()
            .find(|item| item.id.starts_with(id))
            .ok_or_else(|| anyhow::anyhow!("no daemon matching {id}"))?,
        None => {
            let available = all.into_iter().filter(daemon_ready).collect::<Vec<_>>();
            if available.len() == 1 {
                available.into_iter().next().unwrap()
            } else if available.is_empty() {
                anyhow::bail!("no detached xfer daemons found")
            } else {
                println!("{} detached xfer daemons found:", available.len());
                for item in available {
                    println!("{}  pid={}  {}", item.id, item.pid, item.socket.display());
                }
                println!("Use: xfer attach <id>");
                return Ok(());
            }
        }
    };
    let mut stream = UnixStream::connect(&selected.socket)?;
    stream.write_all(b"ATTACH\n")?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.trim() == "BUSY" {
        anyhow::bail!("daemon {} is already attached", selected.id);
    }
    if line.trim() != "ATTACHED" {
        anyhow::bail!("attach failed: {}", line.trim());
    }
    let local_directory = std::env::current_dir()?;
    let local_directory = local_directory
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("local directory must be valid UTF-8"))?;
    stream
        .write_all(format!("LOCAL_CWD {}\n", serde_json::to_string(local_directory)?).as_bytes())?;
    line.clear();
    if reader.read_line(&mut line)? == 0 || line.trim() != "LOCAL_CWD_OK" {
        anyhow::bail!("cannot set daemon local directory: {}", line.trim());
    }
    let mut typeahead = Typeahead::default();
    // Claim startup exactly once per daemon, not on every later attach.
    stream.write_all(b"STARTUP\n")?;
    line.clear();
    if reader.read_line(&mut line)? == 0 {
        anyhow::bail!("daemon disconnected");
    }
    match line.trim() {
        "RUN_STARTUP" => {
            if let Some(home) = std::env::var_os("HOME") {
                let path = std::path::absolute(PathBuf::from(home).join(".xferrc"))?;
                let commands = read_startup_commands(&path)?;
                if !commands.is_empty() {
                    report_startup_file(&mut stream, &mut reader, &path)?;
                }
                for command in commands {
                    // Configuration commands are not interactive history entries.
                    if !dispatch_attached_command(
                        &mut stream,
                        &mut reader,
                        &command,
                        None,
                        &mut typeahead,
                    )? {
                        return Ok(());
                    }
                }
            }
        }
        "STARTUP_DONE" => {}
        response => anyhow::bail!("invalid startup response: {response}"),
    }
    // The daemon owns the configured history location; the client only reads/appends it.
    stream.write_all(b"COMMAND show ui:history-file\n")?;
    line.clear();
    reader.read_line(&mut line)?;
    let history_file = line
        .strip_prefix("OUTPUT ui:history-file: ")
        .and_then(|value| {
            value
                .split_once(" —")
                .map(|(value, _)| value.trim().to_owned())
        });
    line.clear();
    reader.read_line(&mut line)?; // DONE
    println!("attached (type `status` for daemon info, `detach` to release it)");
    let mut history = load_history(history_file.as_deref());
    loop {
        let prompt = fetch_session_prompt(&mut stream, &mut reader)?;
        let Some(command) = read_attached_command(
            &mut stream,
            &mut reader,
            &prompt,
            &mut history,
            &mut typeahead,
        )?
        else {
            break;
        };
        if !dispatch_attached_command(
            &mut stream,
            &mut reader,
            &command,
            history_file.as_deref(),
            &mut typeahead,
        )? {
            break;
        }
    }
    Ok(())
}

fn start_daemon_and_attach() -> Result<()> {
    let executable = std::env::current_exe()?;
    let daemon = executable;
    let child = Command::new(&daemon)
        .arg("--daemon")
        // Local shell commands share the client's foreground group, but Ctrl-C
        // there must never signal the session daemon or its background jobs.
        .process_group(0)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| anyhow::anyhow!("start daemon {}: {error}", daemon.display()))?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let instance = loop {
        if let Some(instance) = xfer::instances()?
            .into_iter()
            .find(|item| item.pid == child.id())
        {
            break instance;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("xferd started but did not publish its socket");
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    attach(&vec!["xfer".into(), "attach".into(), instance.id])
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if std::env::var_os(xfer::ssh_askpass::SOCKET_ENV).is_some() {
        return xfer::ssh_askpass::run(args.get(1).map(String::as_str).unwrap_or(""));
    }
    if args.get(1).map(String::as_str) == Some("--daemon") {
        return xfer::daemon_server::run().await;
    }
    if args.get(1).map(String::as_str) == Some("attach") {
        return attach(&args);
    }
    start_daemon_and_attach()
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    #[test]
    fn pager_frame_is_appended_without_erasing_scrollback() {
        let mut output = Vec::new();
        write_pager_frame(&mut output, &["first".into(), "last".into()]).unwrap();
        assert_eq!(output, b"\r\nfirst\r\nlast");
    }

    #[test]
    fn pager_repaints_changed_rows_without_clearing_or_newlines() {
        let mut display = PagerDisplay::default();
        let lines = vec!["long old line".into(), "unchanged".into()];
        let first = String::from_utf8(display.update(80, 4, &lines, "footer").unwrap()).unwrap();
        assert!(first.starts_with("\x1b[?2026h"));
        assert!(first.ends_with("\x1b[?2026l"));
        assert!(!first.contains("\x1b[2J"));
        assert!(!first.contains(['\r', '\n']));
        assert!(display.update(80, 4, &lines, "footer").unwrap().is_empty());

        let lines = vec!["short".into(), "unchanged".into()];
        let changed = String::from_utf8(display.update(80, 4, &lines, "footer").unwrap()).unwrap();
        assert_eq!(changed, "\x1b[?2026h\x1b[1;1Hshort\x1b[K\x1b[?2026l");
        let removed = String::from_utf8(display.update(80, 4, &[], "footer").unwrap()).unwrap();
        assert!(removed.contains("\x1b[1;1H\x1b[K"));
        assert!(removed.contains("\x1b[2;1H\x1b[K"));
        assert!(!removed.contains("footer"));
    }

    #[test]
    fn pager_repaints_after_resize_or_editor_return() {
        let mut display = PagerDisplay::default();
        let lines = vec!["same content".into()];
        let first = display.update(80, 3, &lines, "footer").unwrap();
        assert!(display.update(80, 3, &lines, "footer").unwrap().is_empty());
        display.invalidate();
        assert_eq!(display.update(80, 3, &lines, "footer").unwrap(), first);
        assert!(!display.update(40, 3, &lines, "footer").unwrap().is_empty());
        assert!(!display.update(40, 4, &lines, "footer").unwrap().is_empty());
    }

    #[test]
    fn pager_sends_one_write_and_flush_per_changed_frame() {
        #[derive(Default)]
        struct Output {
            writes: usize,
            flushes: usize,
        }
        impl Write for Output {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.writes += 1;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                self.flushes += 1;
                Ok(())
            }
        }
        let mut out = Output::default();
        let mut display = PagerDisplay::default();
        display
            .draw(&mut out, 80, 3, &["line".into()], "footer")
            .unwrap();
        assert_eq!((out.writes, out.flushes), (1, 1));
        display
            .draw(&mut out, 80, 3, &["line".into()], "footer")
            .unwrap();
        assert_eq!((out.writes, out.flushes), (1, 1));
    }

    #[test]
    fn pager_keys_cover_line_page_boundary_and_visual_actions() {
        use PagerAction::*;
        for (code, modifiers, expected) in [
            (KeyCode::Char('j'), KeyModifiers::NONE, Down),
            (KeyCode::Down, KeyModifiers::NONE, Down),
            (KeyCode::Char('k'), KeyModifiers::NONE, Up),
            (KeyCode::Up, KeyModifiers::NONE, Up),
            (KeyCode::Char('f'), KeyModifiers::CONTROL, PageDown),
            (KeyCode::Char('b'), KeyModifiers::CONTROL, PageUp),
            (KeyCode::PageDown, KeyModifiers::NONE, PageDown),
            (KeyCode::PageUp, KeyModifiers::NONE, PageUp),
            (KeyCode::Char(' '), KeyModifiers::NONE, PageDown),
            (KeyCode::Home, KeyModifiers::NONE, Home),
            (KeyCode::End, KeyModifiers::NONE, End),
            (KeyCode::Char('|'), KeyModifiers::SHIFT, Visual),
            (KeyCode::Char('q'), KeyModifiers::NONE, Quit),
            (KeyCode::Esc, KeyModifiers::NONE, Quit),
        ] {
            assert_eq!(pager_action(KeyEvent::new(code, modifiers)), Some(expected));
        }
        assert_eq!(
            pager_action(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::ALT)),
            None
        );
        let release =
            KeyEvent::new_with_kind(KeyCode::Down, KeyModifiers::NONE, KeyEventKind::Release);
        assert_eq!(pager_action(release), None);
    }

    #[test]
    fn pager_navigation_is_line_based_clamped_and_resize_safe() {
        use PagerAction::*;
        assert_eq!(pager_offset(Down, 0, 100, 10), 1);
        assert_eq!(pager_offset(Up, 1, 100, 10), 0);
        assert_eq!(pager_offset(PageDown, 1, 100, 10), 11);
        assert_eq!(pager_offset(PageUp, 11, 100, 10), 1);
        assert_eq!(pager_offset(End, 0, 100, 10), 90);
        assert_eq!(pager_offset(Down, 90, 100, 10), 90);
        assert_eq!(pager_offset(PageDown, 85, 100, 10), 90);
        assert_eq!(pager_offset(Home, 90, 100, 10), 0);
        assert_eq!(pager_offset(Up, 0, 100, 10), 0);
        assert_eq!(pager_offset(PageUp, 1, 100, 10), 0);
        assert_eq!(pager_offset(Visual, 90, 100, 20), 80);
        for rows in [0, 1, 5] {
            assert_eq!(pager_offset(End, 0, rows, 10), 0);
        }
        assert_eq!(pager_page_size(0), 1);
        assert_eq!(pager_page_size(24), 23);
    }

    #[test]
    fn pager_disk_index_handles_empty_unterminated_and_chunk_boundary_lines() {
        use std::io::{Read, Seek, SeekFrom};
        for text in ["", "one", "one\n", "\none\n\ntwo", "α\nβ\n"] {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(text.as_bytes()).unwrap();
            let mut index = PagerIndex::new(&mut file).unwrap();
            let expected = text.split_inclusive('\n').collect::<Vec<_>>();
            assert_eq!(index.rows, expected.len());
            for (row, line) in expected.iter().enumerate() {
                file.seek(SeekFrom::Start(index.offset(row).unwrap()))
                    .unwrap();
                let mut actual = vec![0; line.len()];
                file.read_exact(&mut actual).unwrap();
                assert_eq!(actual, line.as_bytes());
            }
        }
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&vec![b'x'; 64 * 1024 - 1]).unwrap();
        file.write_all(b"\nlast\n").unwrap();
        let mut index = PagerIndex::new(&mut file).unwrap();
        assert_eq!(index.rows, 2);
        assert_eq!(index.offset(1).unwrap(), 64 * 1024);
    }

    #[test]
    fn visual_receives_a_private_full_output_copy_and_quoted_arguments() {
        use std::os::unix::fs::PermissionsExt;
        let original = b"first\nsecond\nlast without newline";
        let copy = pager_output_copy(|file| file.write_all(original)).unwrap();
        let path = copy.path().to_owned();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        let command =
            visual_command("'editor with spaces' --flag 'arg with spaces'", &path).unwrap();
        assert_eq!(command.get_program(), "editor with spaces");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![
                std::ffi::OsStr::new("--flag"),
                std::ffi::OsStr::new("arg with spaces"),
                path.as_os_str(),
            ]
        );
        assert!(visual_command("'unterminated", &path).is_err());
        assert!(visual_command(" ", &path).is_err());
        fs::write(&path, "edited").unwrap();
        assert_eq!(original, b"first\nsecond\nlast without newline");
        drop(copy);
        assert!(!path.exists());
    }

    #[test]
    fn foreground_retry_errors_are_logged_once_per_attempt() {
        let mut display = ForegroundDisplay::default();
        let status = format!(
            "[1] Running attempt 1 failed: {}; retrying in 3s",
            "long error ".repeat(30)
        );
        let first = display.update(&status, 80);
        assert_eq!(first.matches("long error").count(), 30);
        assert!(first.contains("retrying in 3s"));
        for _ in 0..30 {
            assert!(display.update(&status, 80).is_empty());
        }
        let resized = display.update(&status, 50);
        assert!(!resized.contains("long error"));
        let next = status.replace("attempt 1", "attempt 2").replace("3s", "6s");
        assert!(display.update(&next, 80).contains("attempt 2 failed"));
        assert!(display.update(&next, 80).is_empty());
    }

    #[test]
    fn status_lines_do_not_wrap_even_with_wide_unicode() {
        for width in [0, 1, 2, 10, 80] {
            let line = fit_status_line("status 界界界\nretry", width);
            assert!(
                unicode_width::UnicodeWidthStr::width(line.as_str())
                    <= width.saturating_sub(1) as usize
            );
            assert!(!line.contains('\n'));
        }
    }

    #[test]
    fn startup_file_is_line_oriented_and_ignores_comments() {
        let path = std::env::temp_dir().join(format!("xfer-rc-test-{}", std::process::id()));
        fs::write(
            &path,
            "# startup\n\nset s3:region eu-west-1\r\n  # comment\nshow s3:region\n",
        )
        .unwrap();
        assert_eq!(
            read_startup_commands(&path).unwrap(),
            vec!["set s3:region eu-west-1", "show s3:region"]
        );
        fs::remove_file(&path).unwrap();
        assert!(read_startup_commands(&path).unwrap().is_empty());
    }
    #[test]
    fn startup_file_is_reported_with_json_escaping_and_acknowledged() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let path = PathBuf::from("/tmp/home with spaces/\"quoted\"/.xferrc");
        let expected = path.clone();
        let server = std::thread::spawn(move || {
            let mut reader = BufReader::new(server);
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            let json = request.strip_prefix("STARTUP_FILE ").unwrap();
            assert_eq!(serde_json::from_str::<PathBuf>(json).unwrap(), expected);
            reader.get_mut().write_all(b"STARTUP_FILE_OK\n").unwrap();
        });
        report_startup_file(&mut client, &mut reader, &path).unwrap();
        server.join().unwrap();
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
    fn prompt_includes_backend_cwd_but_never_daemon_pid() {
        let mut info = xfer::core::SessionInfo {
            daemon_pid: 987654,
            protocol: None,
            cwd: None,
        };
        assert_eq!(session_prompt(&info), "xfer> ");
        info.protocol = Some("s3".into());
        info.cwd = Some("/archive/photos/".into());
        assert_eq!(session_prompt(&info), "xfer[s3:/archive/photos/]> ");
        assert!(!session_prompt(&info).contains("987654"));
        info.cwd = Some("/photos/\x1b[2J\n".into());
        assert!(!session_prompt(&info).contains('\x1b'));
        assert!(!session_prompt(&info).contains('\n'));
    }

    #[test]
    fn prompt_is_loaded_without_creating_a_job() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(server.try_clone().unwrap());
            let mut server = server;
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request, "SESSION\n");
            let info = xfer::core::SessionInfo {
                daemon_pid: 123,
                protocol: Some("s3".into()),
                cwd: Some("/archive/".into()),
            };
            writeln!(server, "SESSION {}", serde_json::to_string(&info).unwrap()).unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        assert_eq!(
            fetch_session_prompt(&mut client, &mut reader).unwrap(),
            "xfer[s3:/archive/]> "
        );
        worker.join().unwrap();
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
        let (mut client, server) = UnixStream::pair().unwrap();
        let candidates = candidates.iter().map(|value| value.to_string()).collect();
        let expected = if command.starts_with("ls ") {
            "COMPLETE_LS "
        } else if command.starts_with("get ") || command.starts_with("cat ") {
            "COMPLETE_GET "
        } else {
            "COMPLETE_CD "
        };
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(server.try_clone().unwrap());
            let mut server = server;
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert!(request.starts_with(expected));
            let result = xfer::cache::PathCompletion {
                candidates,
                truncated,
            };
            writeln!(
                server,
                "COMPLETION {}",
                serde_json::to_string(&result).unwrap()
            )
            .unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut command = command.to_owned();
        complete_path_command(&mut client, &mut reader, &mut command).unwrap();
        worker.join().unwrap();
        command
    }

    #[test]
    fn get_completion_preserves_continue_flag_and_prior_filenames() {
        assert_eq!(
            path_completion_context("get -c first.txt pho").unwrap(),
            (
                "get -c first.txt".into(),
                "pho".into(),
                "COMPLETE_GET".into()
            )
        );
        assert_eq!(
            path_completion_context("get -c ").unwrap(),
            ("get -c".into(), "".into(), "COMPLETE_GET".into())
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
    fn cat_completion_handles_multiple_quoted_paths_and_literal_dash_names() {
        assert_eq!(
            path_completion_context("cat first.txt pho").unwrap(),
            ("cat first.txt".into(), "pho".into(), "COMPLETE_GET".into())
        );
        let completed = complete_with_candidates("cat 'first file' -- -fi", &["-file name"], false);
        assert_eq!(
            shlex::split(&completed).unwrap(),
            vec!["cat", "first file", "--", "-file name"]
        );
    }

    #[test]
    fn raw_output_copy_preserves_binary_bytes_across_chunks_without_pager_or_footer() {
        let chunks = vec![
            vec![0, 255, 10, 13],
            (0..65536).map(|n| (n % 251) as u8).collect::<Vec<_>>(),
            b"no newline".to_vec(),
        ];
        let expected = chunks.concat();
        let (mut client, server) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(server);
            let mut offset = 0;
            for bytes in chunks.into_iter().chain(std::iter::once(Vec::new())) {
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                assert_eq!(request, format!("COMMAND read-output 42 {offset}\n"));
                offset += bytes.len();
                writeln!(
                    reader.get_mut(),
                    "OUTPUT CHUNK {offset} {}\nDONE",
                    serde_json::to_string(&bytes).unwrap()
                )
                .unwrap();
            }
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut output = Vec::new();
        let copied = copy_job_output(&mut client, &mut reader, 42, &mut output).unwrap();
        assert_eq!(output, expected);
        assert_eq!(copied.bytes as usize, expected.len());
        assert_eq!(copied.last, Some(b'e'));
        worker.join().unwrap();
    }

    #[test]
    fn raw_copy_rejects_inconsistent_offsets_even_at_eof() {
        for bytes in [vec![], vec![1, 2], vec![0; 65537]] {
            let (mut client, mut server) = UnixStream::pair().unwrap();
            let worker = std::thread::spawn(move || {
                let mut request = String::new();
                BufReader::new(server.try_clone().unwrap())
                    .read_line(&mut request)
                    .unwrap();
                writeln!(
                    server,
                    "OUTPUT CHUNK 1 {}\nDONE",
                    serde_json::to_string(&bytes).unwrap()
                )
                .unwrap();
            });
            let mut reader = BufReader::new(client.try_clone().unwrap());
            let mut output = Vec::new();
            assert!(copy_job_output(&mut client, &mut reader, 1, &mut output).is_err());
            assert!(output.is_empty());
            worker.join().unwrap();
        }
    }

    #[test]
    fn transfer_bar_includes_throughput_elapsed_and_eta_without_wrapping() {
        let stats = xfer::transfer::TransferProgress {
            file: "file.iso".into(),
            file_index: 1,
            file_count: 1,
            done: 1024,
            total: 2048,
            transferred: 1024,
            elapsed_ms: 2000,
        };
        let line = render_transfer(1, &stats, 512.0, 100);
        assert!(line.contains("50%"));
        assert!(line.contains("512B/s"));
        assert!(line.contains("t00:02"));
        assert!(line.contains("ETA00:02"));
        assert!(line.contains('█'));
        for width in [20, 40, 80, 100] {
            assert!(
                unicode_width::UnicodeWidthStr::width(
                    render_transfer(1, &stats, 512.0, width).as_str()
                ) < width as usize
            );
        }
        assert!(render_transfer(1, &stats, 0.0, 100).contains("ETA--:--"));
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
                (prefix.into(), "pho".into(), "COMPLETE_LS".into())
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
        let (mut client, server) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(server.try_clone().unwrap());
            let mut server = server;
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request, "COMPLETE_CD \"pho\"\n");
            let completion = xfer::cache::PathCompletion {
                candidates: vec!["photo album/".into()],
                truncated: false,
            };
            writeln!(
                server,
                "COMPLETION {}",
                serde_json::to_string(&completion).unwrap()
            )
            .unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut command = "cd pho".to_owned();
        complete_path_command(&mut client, &mut reader, &mut command).unwrap();
        assert_eq!(shlex::split(&command).unwrap(), vec!["cd", "photo album/"]);
        worker.join().unwrap();
    }

    #[test]
    fn completion_at_the_caret_preserves_later_arguments_and_uses_the_renamed_setting() {
        for (text, before, expected_request, response, expected) in [
            (
                "get -c 'photo album' second.txt",
                "get -c 'pho",
                "COMPLETE_GET \"pho\"\n",
                "COMPLETION {\"candidates\":[\"photo album/\"],\"truncated\":false}\n",
                "get -c 'photo album/' second.txt",
            ),
            (
                "set ui:ls-default-opt '-lF'",
                "set ui:ls-default-opt",
                "COMMAND show\n",
                "OUTPUT ui:ls-default-options: -lF — options\nDONE\n",
                "set ui:ls-default-options '-lF'",
            ),
        ] {
            let (mut client, server) = UnixStream::pair().unwrap();
            let worker = std::thread::spawn(move || {
                let mut reader = BufReader::new(server);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                assert_eq!(request, expected_request);
                reader.get_mut().write_all(response.as_bytes()).unwrap();
            });
            let mut reader = BufReader::new(client.try_clone().unwrap());
            let mut command = CommandLine::new(text.into());
            command.key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
            for _ in before.chars() {
                command.key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
            }
            assert_eq!(command.prefix(), before);
            let mut prefix = command.prefix().to_owned();
            complete_command(&mut client, &mut reader, &mut prefix).unwrap();
            command.complete(prefix);
            assert_eq!(command.as_str(), expected);
            worker.join().unwrap();
        }
    }

    #[test]
    fn shell_escapes_are_local_and_record_the_original_command_once() {
        let directory = tempfile::tempdir().unwrap();
        let history = directory.path().join("history");
        let result = directory.path().join("result with spaces");
        let command = format!(
            "!printf '%s' 'some thing' | grep 'some thing' > {}",
            shlex::try_quote(result.to_str().unwrap()).unwrap()
        );
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        assert!(dispatch_attached_command(
            &mut client,
            &mut reader,
            &command,
            history.to_str(),
            &mut Typeahead::default(),
        )
        .unwrap());
        assert_eq!(fs::read_to_string(&result).unwrap(), "some thing\n");
        assert_eq!(fs::read_to_string(history).unwrap(), format!("{command}\n"));
        // No IPC request or daemon job for a local shell command.
        server.set_nonblocking(true).unwrap();
        let mut byte = [0];
        assert_eq!(
            std::io::Read::read(&mut server, &mut byte)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn empty_command_does_not_send_anything_to_daemon() {
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        for command in ["", " ", "\t  "] {
            assert!(dispatch_attached_command(
                &mut client,
                &mut reader,
                command,
                None,
                &mut Typeahead::default()
            )
            .unwrap());
        }
        server.set_nonblocking(true).unwrap();
        let mut byte = [0];
        let error = std::io::Read::read(&mut server, &mut byte).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn passwords_use_a_separate_json_request_not_command_text() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut reader = BufReader::new(server);
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            let json = request.strip_prefix("CONNECT_PASSWORD ").unwrap();
            let credentials: xfer::credentials::PasswordConnect =
                serde_json::from_str(json).unwrap();
            assert_eq!(credentials.target, "server with spaces");
            assert_eq!(
                credentials.password.expose(),
                "secret \"quotes\" \\ slashes"
            );
            assert_eq!(request.lines().count(), 1);
        });
        send_connection_password(
            &mut client,
            "server with spaces".into(),
            xfer::credentials::Password::new("secret \"quotes\" \\ slashes".into()),
        )
        .unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn startup_and_interactive_commands_share_dispatch() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut read = BufReader::new(server.try_clone().unwrap());
            let mut server = server;
            let mut request = String::new();
            read.read_line(&mut request).unwrap();
            assert_eq!(request, "COMMAND set s3:region eu-west-1\n");
            server.write_all(b"OUTPUT region changed\nDONE\n").unwrap();
        });
        let mut reader = BufReader::new(client.try_clone().unwrap());
        assert!(dispatch_attached_command(
            &mut client,
            &mut reader,
            "set s3:region eu-west-1",
            None,
            &mut Typeahead::default()
        )
        .unwrap());
        worker.join().unwrap();
    }
}
