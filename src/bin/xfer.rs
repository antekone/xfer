#![allow(dead_code, unused_imports)] // Legacy inline UI is retained while IPC rendering is migrated.
use std::{
    fs, io,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{
        disable_raw_mode, enable_raw_mode, Clear, ClearType, EnterAlternateScreen,
        LeaveAlternateScreen,
    },
};

use xfer::core::{
    Backend, JobId, JobManager, JobState, Progress, SettingValue, Settings, Sftp, Smb, S3,
};

#[derive(Clone)]
enum UiMode {
    Command,
    Foreground(JobId),
    Pager { lines: Vec<String>, offset: usize },
    Jobs,
}
struct App {
    mode: UiMode,
    command: String,
    history: Vec<String>,
    history_cursor: Option<usize>,
    history_enabled: bool,
    history_path: Option<PathBuf>,
    messages: Vec<String>,
    settings: Settings,
    jobs: JobManager,
    running: bool,
}

fn default_history_path() -> PathBuf {
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(path).join("xfer/history");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".local/state/xfer/history");
    }
    PathBuf::from(".xfer-history")
}

fn history_path(settings: &Settings) -> Option<PathBuf> {
    match settings.text("ui:history-file").unwrap_or("session") {
        "/dev/null" | "session" => None,
        value => Some(PathBuf::from(value)),
    }
}

fn history_enabled(settings: &Settings) -> bool {
    settings.text("ui:history-file") != Some("/dev/null")
}

impl App {
    fn new(settings: Settings) -> Result<Self> {
        let history_enabled = history_enabled(&settings);
        let history_path = history_path(&settings);
        let history = if !history_enabled {
            Vec::new()
        } else {
            match &history_path {
                Some(path) => match fs::read_to_string(path) {
                    Ok(contents) => contents.lines().map(str::to_owned).collect(),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                    Err(error) => return Err(error.into()),
                },
                None => Vec::new(),
            }
        };
        Ok(Self {
            mode: UiMode::Command,
            command: String::new(),
            history,
            history_cursor: None,
            history_enabled,
            history_path,
            messages: vec!["xfer ready; type help".into()],
            settings,
            jobs: JobManager::new(),
            running: true,
        })
    }
    fn record_history(&mut self, input: &str) {
        if !self.history_enabled {
            return;
        }
        self.history.push(input.to_owned());
        if let Some(path) = &self.history_path {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            // History is line-oriented; command input cannot contain a newline.
            let _ = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|mut file| writeln!(file, "{input}"));
        }
    }
    fn execute(&mut self) {
        let input = std::mem::take(&mut self.command);
        self.history_cursor = None;
        if !input.trim().is_empty() {
            self.record_history(&input);
        }
        let mut words = input.split_whitespace();
        let cmd = match words.next() {
            Some(v) => v,
            None => return,
        };
        match cmd {
            "help" => self.messages.extend(
                [
                    "Commands:",
                    "  connect <url>       connect (demo job)",
                    "  get <path>          transfer (demo job)",
                    "  mirror <path>       mirror (demo job)",
                    "  jobs                list jobs",
                    "  fg [id]             foreground a job",
                    "  set <name> <value>  update a setting",
                    "  show <name>         display setting",
                    "  quit                exit",
                    "Keys while foreground: Ctrl-C cancel; Ctrl-Z background.",
                ]
                .into_iter()
                .map(str::to_owned),
            ),
            "connect" | "get" | "mirror" => {
                let known = cmd != "connect";
                let id = self.jobs.start_demo(
                    format!("{cmd} {}", words.collect::<Vec<_>>().join(" ")),
                    known,
                );
                self.mode = UiMode::Foreground(id);
            }
            "jobs" => self.mode = UiMode::Jobs,
            "fg" => {
                let id = words.next().and_then(|s| s.parse().ok()).or_else(|| {
                    self.jobs
                        .jobs
                        .iter()
                        .filter(|(_, j)| j.state == JobState::Running)
                        .map(|(id, _)| *id)
                        .max()
                });
                match id.and_then(|id| self.jobs.jobs.get(&id).map(|job| (id, job.state.clone()))) {
                    Some((id, JobState::Running)) => self.mode = UiMode::Foreground(id),
                    Some((id, state)) => self.messages.push(format!(
                        "job {id} is {state:?}; completed jobs cannot be foregrounded"
                    )),
                    None => self.messages.push("no such job".into()),
                }
            }
            "set" => match (words.next(), words.next()) {
                (Some(name), Some(value)) => match self.settings.set(name, value) {
                    Ok(()) => self.messages.push(format!("{name} set to {value}")),
                    Err(e) => self.messages.push(e.to_string()),
                },
                _ => self.messages.push("usage: set <name> <value>".into()),
            },
            "show" => match words.next() {
                Some(name) => match self.settings.values.get(name) {
                    Some(setting) => self.messages.push(format!(
                        "{name}: {} — {}",
                        setting.value.display(),
                        setting.doc
                    )),
                    None => self.messages.push(format!("unknown setting: {name}")),
                },
                None => {
                    let mut names = self.settings.values.keys().collect::<Vec<_>>();
                    names.sort_unstable();
                    self.messages.extend(names.into_iter().map(|name| {
                        let setting = &self.settings.values[name];
                        format!("{name}: {} — {}", setting.value.display(), setting.doc)
                    }));
                }
            },
            "quit" | "exit" => self.running = false,
            _ => self.messages.push(format!("unknown command: {cmd}")),
        }
    }
}

fn stdout() -> io::Stdout {
    io::stdout()
}

fn prompt(app: &App) {
    let mut out = stdout();
    let _ = write!(out, "xfer> {}", app.command);
    let _ = io::Write::flush(&mut out);
}

fn print_line(line: &str) {
    let mut out = stdout();
    let _ = write!(out, "\r\x1b[2K{line}\r\n");
    let _ = io::Write::flush(&mut out);
}

fn show_jobs(app: &App) {
    for job in app.jobs.jobs.values() {
        print_line(&format!("[{}] {:?}  {}", job.id, job.state, job.title));
    }
    prompt(app);
}

fn draw_pager(lines: &[String], offset: usize) {
    let (_, height) = crossterm::terminal::size().unwrap_or((80, 24));
    let page = height.saturating_sub(1) as usize;
    let mut out = stdout();
    let _ = execute!(out, cursor::MoveTo(0, 0), Clear(ClearType::All));
    for line in lines.iter().skip(offset).take(page) {
        let _ = writeln!(out, "{line}");
    }
    let _ = write!(
        out,
        "-- pager: q/Esc close, arrows/space scroll ({}/{}) --",
        offset + 1,
        lines.len().max(1)
    );
    let _ = io::Write::flush(&mut out);
}

fn enter_pager(app: &mut App, lines: Vec<String>) {
    let mut out = stdout();
    let _ = execute!(out, EnterAlternateScreen, cursor::Hide);
    app.mode = UiMode::Pager { lines, offset: 0 };
    if let UiMode::Pager { lines, offset } = &app.mode {
        draw_pager(lines, *offset);
    }
}

fn leave_pager(app: &mut App) {
    let mut out = stdout();
    let _ = execute!(out, LeaveAlternateScreen, cursor::Show);
    app.mode = UiMode::Command;
    prompt(app);
}

fn render_foreground(app: &App, id: JobId) {
    let Some(job) = app.jobs.jobs.get(&id) else {
        return;
    };
    let status = match job.progress.borrow().clone() {
        Progress::Determinate {
            done,
            total,
            message,
        } => {
            let percent = done * 100 / total.max(1);
            format!(
                "[{}] {:3}% {message}  (^C cancel, ^Z background)",
                job.title, percent
            )
        }
        Progress::Indeterminate { message } => {
            format!("[{}] {message}  (^C cancel, ^Z background)", job.title)
        }
    };
    let mut out = stdout();
    let _ = write!(out, "\r\x1b[2K{status}");
    let _ = io::Write::flush(&mut out);
}

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

fn read_attached_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: &str,
    history: &mut Vec<String>,
) -> Result<Option<String>> {
    enable_raw_mode()?;
    let mut command = String::new();
    let mut history_cursor = None;
    print!("xfer[{id}]> ");
    io::stdout().flush()?;
    loop {
        let Event::Key(key) = event::read()? else {
            continue;
        };
        match key.code {
            KeyCode::Enter => {
                disable_raw_mode()?;
                println!();
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
                    command = history[index].clone();
                    print!("\r\x1b[2Kxfer[{id}]> {command}");
                    io::stdout().flush()?;
                }
            }
            KeyCode::Down => {
                if let Some(index) = history_cursor {
                    if index + 1 < history.len() {
                        history_cursor = Some(index + 1);
                        command = history[index + 1].clone();
                    } else {
                        history_cursor = None;
                        command.clear();
                    }
                    print!("\r\x1b[2Kxfer[{id}]> {command}");
                    io::stdout().flush()?;
                }
            }
            KeyCode::Char('d') if key.modifiers == KeyModifiers::CONTROL && command.is_empty() => {
                disable_raw_mode()?;
                println!();
                return Ok(None);
            }
            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => {
                command.clear();
                print!(
                    "
\x1b[2Kxfer[{id}]> "
                );
                io::stdout().flush()?;
            }
            KeyCode::Backspace => {
                history_cursor = None;
                if command.pop().is_some() {
                    print!("\x08 \x08");
                    io::stdout().flush()?;
                }
            }
            KeyCode::Tab => {
                let matches = if !command.contains(char::is_whitespace) {
                    [
                        "connect",
                        "open",
                        "disconnect",
                        "close",
                        "bookmark",
                        "ls",
                        "get",
                        "mirror",
                        "jobs",
                        "cancel",
                        "show",
                        "set",
                        "help",
                        "detach",
                        "exit",
                    ]
                    .into_iter()
                    .filter(|name| name.starts_with(&command))
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
                } else if command.starts_with("show ") || command.starts_with("set ") {
                    stream.write_all(
                        b"COMMAND show
",
                    )?;
                    let mut choices = Vec::new();
                    let mut line = String::new();
                    loop {
                        line.clear();
                        reader.read_line(&mut line)?;
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
                    let prefix = command.split_once(' ').unwrap().1.trim();
                    choices
                        .into_iter()
                        .filter(|name| name.starts_with(prefix))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if matches.len() == 1 {
                    command = if command.contains(char::is_whitespace) {
                        format!(
                            "{} {}",
                            command.split_whitespace().next().unwrap(),
                            matches[0]
                        )
                    } else {
                        matches[0].clone()
                    };
                    print!("\r\x1b[2Kxfer[{id}]> {command}");
                } else if !matches.is_empty() {
                    print!("\r\n{}\r\nxfer[{id}]> {command}", matches.join("  "));
                }
                io::stdout().flush()?;
            }
            KeyCode::Char(c)
                if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
            {
                history_cursor = None;
                command.push(c);
                print!("{c}");
                io::stdout().flush()?;
            }
            _ => {}
        }
    }
}

struct LocalOutput(std::path::PathBuf);
impl Drop for LocalOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
struct PagerTerminal;
impl Drop for PagerTerminal {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen, cursor::Show);
        let _ = disable_raw_mode();
    }
}

fn display_job_output(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
) -> Result<()> {
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
    let mut offset = 0;
    let mut rows = 0usize;
    loop {
        stream.write_all(format!("COMMAND read-output {id} {offset}\n").as_bytes())?;
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
        if bytes.is_empty() {
            break;
        }
        if next != offset + bytes.len() as u64 {
            anyhow::bail!("invalid output offset");
        }
        rows += bytes.iter().filter(|&&byte| byte == b'\n').count();
        file.write_all(&bytes)?;
        offset = next;
    }
    stream.write_all(b"COMMAND show ui:auto-pager\n")?;
    let mut response = String::new();
    reader.read_line(&mut response)?;
    let auto_pager = response.starts_with("OUTPUT ui:auto-pager: true");
    response.clear();
    reader.read_line(&mut response)?;
    file.seek(SeekFrom::Start(0))?;
    let (_, height) = crossterm::terminal::size().unwrap_or((80, 24));
    if !auto_pager || rows < height.saturating_sub(1) as usize {
        std::io::copy(&mut file, &mut io::stdout())?;
        io::stdout().flush()?;
        return Ok(());
    }
    enable_raw_mode()?;
    let _terminal = PagerTerminal;
    execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
    let mut page_offsets = vec![0];
    let mut page = 0;
    loop {
        let (width, height) = crossterm::terminal::size().unwrap_or((80, 24));
        let mut input = BufReader::new(&mut file);
        input.seek(SeekFrom::Start(page_offsets[page]))?;
        execute!(io::stdout(), cursor::MoveTo(0, 0), Clear(ClearType::All))?;
        for _ in 0..height.saturating_sub(1) {
            let mut line = String::new();
            if input.read_line(&mut line)? == 0 {
                break;
            }
            let visible = line
                .trim_end_matches(['\r', '\n'])
                .chars()
                .take(width.saturating_sub(1) as usize)
                .collect::<String>();
            print!("{visible}\r\n");
        }
        let next = input.stream_position()?;
        let end = next >= offset;
        execute!(io::stdout(), cursor::MoveTo(0, height.saturating_sub(1)))?;
        let label = format!(
            "-- page {}{}; Space/PgDn next, PgUp previous, q exit --",
            page + 1,
            if end { " (end)" } else { "" }
        );
        print!(
            "{}",
            label
                .chars()
                .take(width.saturating_sub(1) as usize)
                .collect::<String>()
        );
        io::stdout().flush()?;
        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char(' ') | KeyCode::PageDown | KeyCode::Down if !end => {
                    page_offsets.truncate(page + 1);
                    page_offsets.push(next);
                    page += 1;
                }
                KeyCode::PageUp | KeyCode::Up => page = page.saturating_sub(1),
                _ => {}
            }
        }
    }
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

fn foreground_job(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
) -> Result<()> {
    enable_raw_mode()?;
    let mut display = ForegroundDisplay::default();
    loop {
        stream.write_all(format!("COMMAND status {id}\n").as_bytes())?;
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let status = line
            .strip_prefix("OUTPUT ")
            .unwrap_or(&line)
            .trim()
            .to_owned();
        line.clear();
        reader.read_line(&mut line)?;
        let state = status.split_whitespace().nth(1);
        if matches!(state, Some("Completed" | "Cancelled" | "Failed")) || status == "no such job" {
            print!("\r\x1b[2K{status}");
            io::stdout().flush()?;
            disable_raw_mode()?;
            println!();
            if status != "no such job" {
                display_job_output(stream, reader, id)?;
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
        let redraw = display.update(&status, width);
        if !redraw.is_empty() {
            print!("{redraw}");
            io::stdout().flush()?;
        }
        if event::poll(Duration::from_millis(150))? {
            if let Event::Key(key) = event::read()? {
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
                            reader.read_line(&mut line)?;
                            if line.trim() == "DONE" {
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Shared execution path for rc lines and interactive commands.
fn dispatch_attached_command(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    command: &str,
    history_file: Option<&str>,
) -> Result<bool> {
    if command.trim().is_empty() {
        return Ok(true);
    }
    let mut line = String::new();
    if matches!(command.trim(), "detach" | "exit") {
        stream.write_all(b"DETACH\n")?;
        return Ok(false);
    }
    if command.trim() == "bg" {
        println!("all jobs are already in the background");
        return Ok(true);
    }
    if let Some(id) = command
        .trim()
        .strip_prefix("fg ")
        .and_then(|value| value.trim().parse().ok())
    {
        foreground_job(stream, reader, id)?;
        return Ok(true);
    }
    if command.trim() == "fg" {
        println!("usage: fg <job-id>");
        return Ok(true);
    }
    if !command.trim().is_empty() {
        append_history(history_file, command);
    }
    stream.write_all(format!("COMMAND {command}\n").as_bytes())?;
    let mut foreground = None;
    let mut edit_bookmarks = None;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            anyhow::bail!("daemon disconnected");
        }
        if line.trim() == "DONE" {
            break;
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
        foreground_job(stream, reader, id)?;
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
    // Claim startup exactly once per daemon, not on every later attach.
    stream.write_all(b"STARTUP\n")?;
    line.clear();
    if reader.read_line(&mut line)? == 0 {
        anyhow::bail!("daemon disconnected");
    }
    match line.trim() {
        "RUN_STARTUP" => {
            if let Some(home) = std::env::var_os("HOME") {
                for command in read_startup_commands(&PathBuf::from(home).join(".xferrc"))? {
                    // Configuration commands are not interactive history entries.
                    if !dispatch_attached_command(&mut stream, &mut reader, &command, None)? {
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
    println!(
        "attached to daemon {} (type `detach` to release it)",
        selected.id
    );
    let mut history = load_history(history_file.as_deref());
    while let Some(command) =
        read_attached_command(&mut stream, &mut reader, &selected.id, &mut history)?
    {
        if !dispatch_attached_command(&mut stream, &mut reader, &command, history_file.as_deref())?
        {
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
    if args.get(1).map(String::as_str) == Some("--daemon") {
        return xfer::daemon_server::run().await;
    }
    if args.get(1).map(String::as_str) == Some("attach") {
        return attach(&args);
    }
    start_daemon_and_attach()
}

async fn run(mut app: App) -> Result<()> {
    prompt(&app);
    while app.running {
        for id in app.jobs.poll() {
            if matches!(app.mode, UiMode::Foreground(active) if active == id) {
                let output = app
                    .jobs
                    .jobs
                    .get(&id)
                    .map(|j| j.output.read_all())
                    .unwrap_or_default();
                print_line("");
                let (_, terminal_height) = crossterm::terminal::size().unwrap_or((80, 24));
                if app.settings.bool("ui:auto-pager") && output.len() > terminal_height as usize {
                    enter_pager(&mut app, output);
                } else {
                    for line in output {
                        print_line(&line);
                    }
                    app.mode = UiMode::Command;
                    prompt(&app);
                }
            }
        }
        if let UiMode::Foreground(id) = app.mode {
            render_foreground(&app, id);
        }
        if event::poll(Duration::from_millis(80))? {
            if let Event::Key(key) = event::read()? {
                handle_key(&mut app, key);
            }
        }
    }
    print_line("");
    Ok(())
}

fn execute_command(app: &mut App) {
    let before = app.messages.len();
    app.execute();
    match app.mode.clone() {
        UiMode::Pager { lines, .. } => enter_pager(app, lines),
        UiMode::Jobs => {
            app.mode = UiMode::Command;
            show_jobs(app);
        }
        UiMode::Foreground(_) => {}
        UiMode::Command => {
            for line in &app.messages[before..] {
                print_line(line);
            }
            prompt(app);
        }
    }
}

fn redraw_prompt(app: &App) {
    let mut out = stdout();
    let _ = write!(out, "\r\x1b[2K");
    let _ = io::Write::flush(&mut out);
    prompt(app);
}

fn complete_show_setting(app: &mut App) {
    let Some(rest) = app.command.strip_prefix("show") else {
        return;
    };
    // Only complete the first/only argument; this simple prompt editor has no mid-line cursor yet.
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return;
    }
    let prefix = rest.trim();
    let mut candidates = app
        .settings
        .values
        .keys()
        .filter(|name| name.starts_with(prefix))
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    match candidates.len() {
        0 => {}
        1 => {
            app.command = format!("show {}", candidates[0]);
            redraw_prompt(app);
        }
        _ => {
            // Display candidates without leaving the ordinary prompt buffer, like readline/lftp.
            print_line("");
            print_line(&candidates.join("  "));
            prompt(app);
        }
    }
}

fn history_up(app: &mut App) {
    let Some(last) = app.history.len().checked_sub(1) else {
        return;
    };
    let next = app
        .history_cursor
        .map(|index| index.saturating_sub(1))
        .unwrap_or(last);
    app.history_cursor = Some(next);
    app.command = app.history[next].clone();
    redraw_prompt(app);
}

fn history_down(app: &mut App) {
    let Some(index) = app.history_cursor else {
        return;
    };
    if index + 1 < app.history.len() {
        app.history_cursor = Some(index + 1);
        app.command = app.history[index + 1].clone();
    } else {
        app.history_cursor = None;
        app.command.clear();
    }
    redraw_prompt(app);
}

fn handle_key(app: &mut App, key: KeyEvent) {
    match app.mode.clone() {
        UiMode::Foreground(id) => match (key.code, key.modifiers) {
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => app.jobs.cancel(id),
            (KeyCode::Char('z'), KeyModifiers::CONTROL) => {
                print_line("");
                app.messages.push(format!("job {id} moved to background"));
                app.mode = UiMode::Command;
                print_line(&format!("job {id} moved to background"));
                prompt(app);
            }
            _ => {}
        },
        UiMode::Pager { lines, mut offset } => {
            let (_, height) = crossterm::terminal::size().unwrap_or((80, 24));
            let page = height.saturating_sub(1) as usize;
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => leave_pager(app),
                KeyCode::Down => {
                    offset = (offset + 1).min(lines.len().saturating_sub(1));
                    app.mode = UiMode::Pager { lines, offset };
                    if let UiMode::Pager { lines, offset } = &app.mode {
                        draw_pager(lines, *offset);
                    }
                }
                KeyCode::Up => {
                    offset = offset.saturating_sub(1);
                    app.mode = UiMode::Pager { lines, offset };
                    if let UiMode::Pager { lines, offset } = &app.mode {
                        draw_pager(lines, *offset);
                    }
                }
                KeyCode::PageDown | KeyCode::Char(' ') => {
                    offset = (offset + page).min(lines.len().saturating_sub(1));
                    app.mode = UiMode::Pager { lines, offset };
                    if let UiMode::Pager { lines, offset } = &app.mode {
                        draw_pager(lines, *offset);
                    }
                }
                _ => {}
            }
        }
        UiMode::Jobs => {}
        UiMode::Command => match key.code {
            KeyCode::Tab => complete_show_setting(app),
            KeyCode::Up => history_up(app),
            KeyCode::Down => history_down(app),
            KeyCode::Enter => {
                print_line("");
                execute_command(app);
            }
            KeyCode::Backspace => {
                app.history_cursor = None;
                if app.command.pop().is_some() {
                    let mut out = stdout();
                    let _ = write!(out, "\x08 \x08");
                    let _ = io::Write::flush(&mut out);
                }
            }
            // Readline-like word erase: remove trailing whitespace, then the preceding word.
            KeyCode::Char('w') if key.modifiers == KeyModifiers::CONTROL => {
                app.history_cursor = None;
                let trimmed = app.command.trim_end();
                let word_start = trimmed
                    .rfind(char::is_whitespace)
                    .map(|index| index + 1)
                    .unwrap_or(0);
                app.command.truncate(word_start);
                redraw_prompt(app);
            }
            KeyCode::Char(c)
                if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
            {
                app.command.push(c);
                let mut out = stdout();
                let _ = write!(out, "{c}");
                let _ = io::Write::flush(&mut out);
            }
            // Ctrl-C is never an application exit. In command mode it abandons the current input;
            // in foreground-job mode it is handled above and cancels that job.
            KeyCode::Char('c') if key.modifiers == KeyModifiers::CONTROL => {
                if !app.command.is_empty() {
                    app.command.clear();
                    app.history_cursor = None;
                    let mut out = stdout();
                    let _ = write!(out, "\r\x1b[2K");
                    let _ = io::Write::flush(&mut out);
                    prompt(app);
                }
            }
            // Match shell/readline EOF semantics: only an empty prompt accepts Ctrl-D as exit.
            KeyCode::Char('d')
                if key.modifiers == KeyModifiers::CONTROL && app.command.is_empty() =>
            {
                app.running = false
            }
            _ => {}
        },
    }
}

#[cfg(test)]
mod startup_tests {
    use super::*;
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
    fn empty_command_does_not_send_anything_to_daemon() {
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        for command in ["", " ", "\t  "] {
            assert!(dispatch_attached_command(&mut client, &mut reader, command, None).unwrap());
        }
        server.set_nonblocking(true).unwrap();
        let mut byte = [0];
        let error = std::io::Read::read(&mut server, &mut byte).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
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
            None
        )
        .unwrap());
        worker.join().unwrap();
    }
}
