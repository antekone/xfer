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
                        "connect", "get", "mirror", "jobs", "cancel", "show", "set", "help",
                        "detach",
                    ]
                    .into_iter()
                    .filter(|name| name.starts_with(&command))
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
                } else if command == "show" || command.starts_with("show ") {
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
                    let prefix = command.strip_prefix("show").unwrap().trim();
                    choices
                        .into_iter()
                        .filter(|name| name.starts_with(prefix))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if matches.len() == 1 {
                    command = if command.contains(char::is_whitespace) {
                        format!("show {}", matches[0])
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

fn foreground_job(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    id: u64,
) -> Result<()> {
    enable_raw_mode()?;
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
        print!("\r\x1b[2K{status}  (^C cancel, ^Z background)");
        io::stdout().flush()?;
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
        if status.contains("Completed") || status.contains("Cancelled") || status.contains("Failed")
        {
            // Output retention is acknowledged by the pager/output view, not by foregrounding.
            disable_raw_mode()?;
            println!();
            return Ok(());
        }
    }
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
        if matches!(command.trim(), "detach" | "exit") {
            stream.write_all(b"DETACH\n")?;
            break;
        }
        if command.trim() == "bg" {
            println!("all jobs are already in the background");
            continue;
        }
        if let Some(id) = command
            .trim()
            .strip_prefix("fg ")
            .and_then(|value| value.trim().parse().ok())
        {
            foreground_job(&mut stream, &mut reader, id)?;
            continue;
        }
        if command.trim() == "fg" {
            println!("usage: fg <job-id>");
            continue;
        }
        if !command.trim().is_empty() {
            append_history(history_file.as_deref(), &command);
        }
        stream.write_all(format!("COMMAND {command}\n").as_bytes())?;
        let mut foreground = None;
        loop {
            line.clear();
            reader.read_line(&mut line)?;
            if line.trim() == "DONE" {
                break;
            }
            if let Some(id) = line
                .strip_prefix("OUTPUT JOB ")
                .and_then(|value| value.trim().parse().ok())
            {
                foreground = Some(id);
            }
            print!("{}", line.strip_prefix("OUTPUT ").unwrap_or(&line));
        }
        if let Some(id) = foreground {
            foreground_job(&mut stream, &mut reader, id)?;
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
