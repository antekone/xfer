mod cli;
mod daemon_client;
mod file_selector;
mod line_reader;
#[cfg(test)]
mod test_support;

// ------------------------------------------------------------------------------------------------

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
use line_reader::{
    history_persist_append, history_persist_load, read_attached_command, read_connection_password,
    RawInputGuard, Typeahead,
};
use std::env;
use std::{
    fs, io,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};
use xfer::commands::{
    self, ClientApp, CommandContext, CommandResult, ExecutionSite, ParserContext,
};
use xfer::ipc::{self, pb, Op, Reply};
use xfer::runtime_dir;
type Connection = ipc::Client<UnixStream>;

// ------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Instance {
    pub id: String,
    pub pid: u32,
    pub socket: PathBuf,
}

// ------------------------------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // SSH selects the askpass bridge through the environment and passes its
    // prompt as argv[1]; it must never go through argument parsing.
    if env::var_os(xfer::ssh_askpass::SOCKET_ENV).is_some() {
        return xfer::ssh_askpass::main(&env::args().nth(1).unwrap_or_default());
    }

    match cli::parse() {
        cli::Invocation::StartAndAttach => start_daemon_and_attach(),
        cli::Invocation::Daemon => xfer::daemon::main().await,
        cli::Invocation::Attach(id) => attach(id.as_deref()),
    }
}

// ------------------------------------------------------------------------------------------------

fn start_daemon_and_attach() -> Result<()> {
    attached_session(daemon_client::start()?)
}

fn attach(id: Option<&str>) -> Result<()> {
    let all = instances()?;

    let selected = match id {
        Some(id) => select_instance(all, id)?,
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

                println!("use: xfer attach <id>");
                return Ok(());
            }
        }
    };

    let stream = UnixStream::connect(&selected.socket)?;
    attached_session(claim_attachment(stream, Duration::from_secs(5))?)
}

fn select_instance(instances: Vec<Instance>, id: &str) -> Result<Instance> {
    if let Some(exact) = instances.iter().find(|instance| instance.id == id) {
        return Ok(exact.clone());
    }

    let mut matches = instances
        .into_iter()
        .filter(|instance| instance.id.starts_with(id));

    let selected = matches
        .next()
        .ok_or_else(|| anyhow::anyhow!("no daemon matching {id}"))?;

    if matches.next().is_some() {
        anyhow::bail!("ambiguous daemon ID {id}; use a longer prefix");
    }

    Ok(selected)
}

/// Claim the lease before treating startup as successful. A fresh child's
/// lifetime guard stays armed until this handshake completes.
fn claim_attachment(stream: UnixStream, timeout: Duration) -> Result<Connection> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let mut client = Connection::new(stream);
    client.attach()?;

    client.transport().set_read_timeout(None)?;
    client.transport().set_write_timeout(None)?;
    Ok(client)
}

fn attached_session(mut client: Connection) -> Result<()> {
    let local_directory = std::env::current_dir()?;
    let local_directory = local_directory
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("local directory must be valid UTF-8"))?;

    client.ok(Op::SetLocalCwd(pb::Path {
        path: local_directory.into(),
    }))?;

    let mut typeahead = Typeahead::default();

    let run_startup = match client.call(Op::ClaimStartup(pb::Empty {}))? {
        Reply::Startup(result) => result.run,
        _ => return client.unexpected(),
    };

    if run_startup {
        if let Some(home) = xfer::home_dir() {
            let path = std::path::absolute(home.join(".xferrc"))?;
            let commands = read_startup_commands(&path)?;
            if !commands.is_empty() {
                report_startup_file(&mut client, &path)?;
            }
            for command in commands {
                if !dispatch_attached_command(&mut client, &command, None, &mut typeahead)? {
                    return Ok(());
                }
            }
        }
    }

    let history_file = client
        .settings(Some("ui:history-file"))?
        .into_iter()
        .next()
        .and_then(|setting| match setting.value {
            Some(pb::setting::Value::Text(value)) => Some(value),
            _ => None,
        });

    println!("attached (type `status` for daemon info, `detach` to release it)");

    let mut history = history_persist_load(history_file.as_deref());
    loop {
        let prompt = fetch_session_prompt(&mut client)?;

        let Some(command) =
            read_attached_command(&mut client, &prompt, &mut history, &mut typeahead)?
        else {
            break;
        };

        if !dispatch_attached_command(
            &mut client,
            &command,
            history_file.as_deref(),
            &mut typeahead,
        )? {
            break;
        }
    }

    Ok(())
}

// ------------------------------------------------------------------------------------------------

pub fn instances() -> io::Result<Vec<Instance>> {
    instances_in(&runtime_dir()?)
}

fn instances_in(directory: &std::path::Path) -> io::Result<Vec<Instance>> {
    let mut result = Vec::new();

    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|v| v.to_str()) != Some("meta") {
            continue;
        }

        if let Some(instance) = read_instance(&path) {
            if instance.socket.exists() {
                result.push(instance);
            }
        }
        // Enumeration is read-only: a check followed by unlink could delete a
        // newly published entry. Stale cleanup requires separate coordination.
    }

    result.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(result)
}

fn read_instance(path: &std::path::Path) -> Option<Instance> {
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)
        .ok()?
        .take(8193)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > 8192 {
        return None;
    }
    let instance = serde_json::from_slice::<Instance>(&bytes)
        .ok()
        .or_else(|| {
            // Keep explicit attach compatible with daemons started before JSON
            // metadata and per-instance socket directories were introduced.
            let text = std::str::from_utf8(&bytes).ok()?;
            let (mut id, mut pid, mut socket) = (None, None, None);
            for line in text.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    match key {
                        "id" => id = Some(value.to_owned()),
                        "pid" => pid = value.parse().ok(),
                        "socket" => socket = Some(PathBuf::from(value)),
                        _ => {}
                    }
                }
            }
            Some(Instance {
                id: id?,
                pid: pid?,
                socket: socket?,
            })
        })?;
    if instance.id.is_empty() || instance.pid == 0 || !instance.socket.is_absolute() {
        return None;
    }
    Some(instance)
}

// ------------------------------------------------------------------------------------------------

// ------------------------------------------------------------------------------------------------

fn daemon_ready(instance: &Instance) -> bool {
    let Ok(stream) = UnixStream::connect(&instance.socket) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    Connection::new(stream).probe().unwrap_or(false)
}

// ------------------------------------------------------------------------------------------------

struct LocalOutput(std::path::PathBuf);
impl Drop for LocalOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
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
    client: &mut Connection,
    id: u64,
    output: &mut dyn Write,
) -> Result<CopiedOutput> {
    let mut copied = CopiedOutput {
        bytes: 0,
        rows: 0,
        last: None,
    };
    loop {
        let chunk = client.output(id, copied.bytes)?;
        let next = chunk.next_offset;
        let bytes = &chunk.data;
        if bytes.is_empty() {
            break;
        }
        output.write_all(bytes)?;
        copied.bytes = next;
        copied.rows += bytes.iter().filter(|&&byte| byte == b'\n').count();
        copied.last = bytes.last().copied();
        if chunk.eof {
            break;
        }
    }
    output.flush()?;
    Ok(copied)
}

fn display_job_output(client: &mut Connection, id: u64) -> Result<bool> {
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
    let copied = copy_job_output(client, id, &mut file)?;
    let auto_pager = client
        .settings(Some("ui:auto-pager"))?
        .into_iter()
        .next()
        .is_some_and(|setting| setting.value == Some(pb::setting::Value::Boolean(true)));
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

/// Prompts are clipped in the middle rather than from the right: the opening
/// identifies the session and the trailing marker (typically `> `) signals
/// that input is ready. Dropping the tail makes a connected client look like a
/// dead one, which is exactly what happens with long canonicalized macOS
/// paths under /private/var on an otherwise unchanged session.
fn fit_prompt(text: &str, budget: u16) -> String {
    use unicode_width::UnicodeWidthChar;
    let budget = budget as usize;
    let clean = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    if budget == 0 {
        return String::new();
    }
    let width = clean
        .chars()
        .map(|ch| ch.width().unwrap_or(0))
        .sum::<usize>();
    if width <= budget {
        return clean;
    }
    // One cell goes to the ellipsis; keep a shorter head so that the more
    // meaningful tail, including the marker, survives very long values.
    let keep = budget - 1;
    let head_budget = keep / 3;
    let tail_budget = keep - head_budget;
    let mut head = String::new();
    let mut used = 0;
    for ch in clean.chars() {
        let width = ch.width().unwrap_or(0);
        if used + width > head_budget {
            break;
        }
        head.push(ch);
        used += width;
    }
    let mut tail = String::new();
    let mut used = 0;
    for ch in clean.chars().rev() {
        let width = ch.width().unwrap_or(0);
        if used + width > tail_budget {
            break;
        }
        tail.insert(0, ch);
        used += width;
    }
    format!("{head}…{tail}")
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

fn foreground_job(client: &mut Connection, id: u64, typeahead: &mut Typeahead) -> Result<()> {
    enable_raw_mode()?;
    let _raw_input = RawInputGuard;
    let mut display = ForegroundDisplay::default();
    let mut transfer_rate = TransferRate::default();
    loop {
        let job = match client.job(id) {
            Ok(job) => job,
            Err(error)
                if error
                    .downcast_ref::<ipc::RemoteError>()
                    .is_some_and(|error| error.code == pb::ErrorCode::NotFound) =>
            {
                print!("\r\x1b[2K");
                disable_raw_mode()?;
                println!("no such job");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let state =
            pb::JobState::try_from(job.state).map_err(|_| anyhow::anyhow!("invalid job state"))?;
        let message = match &job.progress {
            Some(pb::job::Progress::Indeterminate(value)) => value.message.clone(),
            Some(pb::job::Progress::Determinate(value)) => {
                format!("{}/{} {}", value.done, value.total, value.message)
            }
            Some(pb::job::Progress::Transfer(value)) => value.file.clone(),
            None => String::new(),
        };
        let human_status = format!("[{id}] {state:?} {message}");
        if matches!(
            state,
            pb::JobState::Completed | pb::JobState::Cancelled | pb::JobState::Failed
        ) {
            print!("\r\x1b[2K");
            io::stdout().flush()?;
            disable_raw_mode()?;
            if job.raw_output {
                let copied = copy_job_output(client, id, &mut io::stdout())?;
                use std::io::IsTerminal;
                if io::stdout().is_terminal() && copied.last.is_some_and(|byte| byte != b'\n') {
                    println!();
                }
                if state != pb::JobState::Completed {
                    eprintln!("{human_status}");
                }
            } else if !display_job_output(client, id)? && state != pb::JobState::Completed {
                println!("{human_status}");
            }
            if let Err(error) = client.ok(Op::AcknowledgeOutput(pb::JobId { id })) {
                if error.is::<ipc::RemoteError>() {
                    eprintln!("{error}");
                } else {
                    return Err(error);
                }
            }
            return Ok(());
        }
        let width = crossterm::terminal::size()
            .map(|(width, _)| width)
            .ok()
            .filter(|width| *width > 0)
            .unwrap_or(80);
        let transfer_status = if let Some(pb::job::Progress::Transfer(value)) = job.progress {
            let stats: xfer::transfer::TransferProgress = value.into();
            Some(render_transfer(
                id,
                &stats,
                transfer_rate.sample(&stats),
                width,
            ))
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
                            client.ok(Op::Cancel(pb::JobId { id }))?;
                        }
                        _ => {}
                    }
                }
            }
            typeahead.capture(event);
        }
    }
}

fn send_connection_password(
    client: &mut Connection,
    target: String,
    password: xfer::credentials::Password,
) -> Result<pb::CommandResult> {
    client.command_reply(Op::ConnectPassword(pb::PasswordConnect {
        target,
        password: password.expose().into(),
    }))
}

/// Read daemon state for every shell escape, not a stale attach-time snapshot.
fn read_local_directory(client: &mut Connection) -> Result<PathBuf> {
    client.local_cwd()
}

struct AttachedCommandContext<'a> {
    client: &'a mut Connection,
    typeahead: &'a mut Typeahead,
}
impl ClientApp for AttachedCommandContext<'_> {
    fn detach(&mut self) -> Result<()> {
        self.client.detach()
    }
    fn foreground(&mut self, id: u64) -> Result<()> {
        foreground_job(self.client, id, self.typeahead)
    }
    fn run_shell(&mut self, source: &str) -> Result<std::process::ExitStatus> {
        let directory = read_local_directory(self.client)?;
        xfer::local_shell::run(source, &directory)
    }
}

/// Shared execution path for rc lines and interactive commands.
fn dispatch_attached_command(
    client: &mut Connection,
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
            eprintln!("{error}");
            return Ok(true);
        }
    };
    let definition = commands::find(&parser);
    if definition.is_none_or(|command| command.record_history_for(&parser)) {
        history_persist_append(history_file, command);
    }
    if let Some(definition) = definition.filter(|command| command.site() == ExecutionSite::Client) {
        let mut app = AttachedCommandContext { client, typeahead };
        match definition.execute(&mut CommandContext::Client { app: &mut app }, &parser) {
            Ok(CommandResult::Detach) => return Ok(false),
            Ok(CommandResult::Output(lines)) => {
                for line in lines {
                    println!("{line}");
                }
            }
            Err(error) => eprintln!("{error:#}"),
            Ok(_) => anyhow::bail!("invalid result from client command"),
        }
        return Ok(true);
    }
    let mut result = match client.command(command) {
        Ok(result) => result,
        Err(error) if error.is::<ipc::RemoteError>() => {
            eprintln!("{error}");
            return Ok(true);
        }
        Err(error) => return Err(error),
    };
    let mut foreground = None;
    let mut edit_bookmarks = None;
    let mut password_sent = false;
    loop {
        for line in result.lines {
            println!("{line}");
        }
        match result.action {
            Some(pb::command_result::Action::JobStarted(job)) => {
                foreground = Some(job.id);
                break;
            }
            Some(pb::command_result::Action::EditBookmarks(path)) => {
                edit_bookmarks = Some(path.path);
                break;
            }
            Some(pb::command_result::Action::PasswordRequired(prompt)) => {
                if password_sent {
                    anyhow::bail!("daemon requested a password twice");
                }
                typeahead.clear();
                let Some(password) = read_connection_password(&prompt.target)? else {
                    return Ok(true);
                };
                result = send_connection_password(client, prompt.target, password)?;
                password_sent = true;
            }
            None => break,
        }
    }
    if let Some(path) = edit_bookmarks {
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .unwrap_or_else(|_| "vi".into());
        match shlex::split(&editor).filter(|args| !args.is_empty()) {
            Some(args) => match Command::new(&args[0]).args(&args[1..]).arg(&path).status() {
                Ok(status) if status.success() => {
                    for line in client.command("bookmark reload")?.lines {
                        println!("{line}");
                    }
                }
                Ok(status) => eprintln!("editor exited with {status}"),
                Err(error) => eprintln!("cannot run editor: {error}"),
            },
            None => eprintln!("invalid VISUAL/EDITOR command"),
        }
    }
    if let Some(id) = foreground {
        foreground_job(client, id, typeahead)?;
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

fn report_startup_file(client: &mut Connection, path: &std::path::Path) -> Result<()> {
    client.ok(Op::StartupFile(pb::Path {
        path: path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("startup path is not UTF-8"))?
            .into(),
    }))
}

fn session_prompt(info: &xfer::daemon::SessionInfo) -> String {
    if let Some(prompt) = &info.prompt {
        return prompt.clone();
    }
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

fn fetch_session_prompt(client: &mut Connection) -> Result<String> {
    Ok(session_prompt(&client.session()?.into()))
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use crate::test_support::rpc_pair;

    fn text_reply(text: &str) -> Reply {
        Reply::Command(pb::CommandResult {
            lines: vec![text.into()],
            action: None,
        })
    }

    #[test]
    fn attachment_handshake_is_bounded_and_sends_no_session_data_before_acceptance() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let started = Instant::now();
        assert!(claim_attachment(stream, Duration::from_millis(100)).is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        let request: pb::Request = ipc::read_frame(&mut peer).unwrap().unwrap();
        assert!(matches!(request.operation, Some(Op::Attach(_))));
        assert!(ipc::read_frame::<pb::Request>(&mut peer).unwrap().is_none());
    }

    #[test]
    fn explicit_attach_rejects_ambiguous_prefixes_and_prefers_exact_ids() {
        let instance = |id: &str| Instance {
            id: id.into(),
            pid: 1,
            socket: "/unused".into(),
        };
        let entries = vec![instance("123-first"), instance("123-second")];
        assert!(select_instance(entries.clone(), "123")
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        assert_eq!(
            select_instance(entries.clone(), "123-f").unwrap().id,
            "123-first"
        );
        assert!(select_instance(entries, "missing").is_err());
        assert_eq!(
            select_instance(vec![instance("123-longer"), instance("123")], "123")
                .unwrap()
                .id,
            "123"
        );
    }

    #[test]
    fn discovery_reads_json_and_legacy_metadata_but_never_deletes_stale_entries() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("s");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        fs::write(
            root.path().join("new.meta"),
            serde_json::json!({"id": "new", "pid": 42, "socket": path}).to_string(),
        )
        .unwrap();
        fs::write(
            root.path().join("old.meta"),
            format!("id=old\npid=24\nsocket={}\n", path.display()),
        )
        .unwrap();
        let stale = root.path().join("stale.meta");
        fs::write(
            &stale,
            serde_json::json!({"id": "stale", "pid": 1, "socket": root.path().join("missing")})
                .to_string(),
        )
        .unwrap();
        fs::write(root.path().join("partial.meta"), "{\"id\":").unwrap();
        fs::write(root.path().join("oversized.meta"), vec![b'x'; 9000]).unwrap();
        let instances = instances_in(root.path()).unwrap();
        assert_eq!(
            instances
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["new", "old"]
        );
        assert!(stale.exists());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 6);
    }
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
    fn startup_file_is_reported_with_protobuf_and_acknowledged() {
        let path = PathBuf::from("/tmp/home with spaces/\"quoted\"/.xferrc");
        let expected = path.clone();
        let (mut client, worker) = rpc_pair(1, move |op| {
            assert_eq!(
                op,
                Op::StartupFile(pb::Path {
                    path: expected.to_str().unwrap().into()
                })
            );
            Reply::Ok(pb::Empty {})
        });
        report_startup_file(&mut client, &path).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn prompt_includes_backend_cwd_but_never_daemon_pid() {
        let mut info = xfer::daemon::SessionInfo {
            daemon_pid: 987654,
            protocol: None,
            cwd: None,
            ..Default::default()
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
        let (mut client, worker) = rpc_pair(1, |op| {
            assert!(matches!(op, Op::Session(_)));
            Reply::Session(pb::Session {
                daemon_pid: 123,
                protocol: Some("s3".into()),
                cwd: Some("/archive/".into()),
                prompt: Some("custom[archive]> ".into()),
                ..Default::default()
            })
        });
        assert_eq!(
            fetch_session_prompt(&mut client).unwrap(),
            "custom[archive]> "
        );
        worker.join().unwrap();
    }

    #[test]
    fn local_directory_lookup_reads_fresh_typed_state() {
        let directories = vec![
            PathBuf::from("/tmp/Pobrane"),
            PathBuf::from("/tmp/photo 'album'\n日本語"),
        ];
        let mut replies = directories.clone().into_iter();
        let (mut client, worker) = rpc_pair(2, move |op| {
            assert!(matches!(op, Op::LocalCwd(_)));
            Reply::LocalCwd(pb::Path {
                path: replies.next().unwrap().to_str().unwrap().into(),
            })
        });
        for directory in directories {
            assert_eq!(read_local_directory(&mut client).unwrap(), directory);
        }
        worker.join().unwrap();
    }

    #[test]
    fn local_directory_lookup_errors_never_fall_back_to_the_clients_startup_directory() {
        for reply in [
            Reply::Ok(pb::Empty {}),
            Reply::LocalCwd(pb::Path {
                path: "relative/path".into(),
            }),
            Reply::Error(pb::Error {
                code: pb::ErrorCode::OperationFailed as i32,
                message: "unavailable".into(),
            }),
        ] {
            let (mut client, worker) = rpc_pair(1, move |op| {
                assert!(matches!(op, Op::LocalCwd(_)));
                reply.clone()
            });
            assert!(read_local_directory(&mut client).is_err());
            worker.join().unwrap();
        }
    }

    #[test]
    fn raw_output_copy_preserves_binary_bytes_across_chunks_without_pager_or_footer() {
        let chunks = vec![
            vec![0, 255, 10, 13],
            (0..65536).map(|n| (n % 251) as u8).collect::<Vec<_>>(),
            b"no newline".to_vec(),
        ];
        let expected = chunks.concat();
        let mut chunks = chunks.into_iter();
        let mut offset = 0;
        let (mut client, worker) = rpc_pair(4, move |op| {
            assert_eq!(op, Op::ReadOutput(pb::OutputRequest { job_id: 42, offset }));
            let data = chunks.next().unwrap_or_default();
            offset += data.len() as u64;
            Reply::Output(pb::OutputChunk {
                next_offset: offset,
                eof: data.is_empty(),
                data,
            })
        });
        let mut output = Vec::new();
        let copied = copy_job_output(&mut client, 42, &mut output).unwrap();
        assert_eq!(output, expected);
        assert_eq!(copied.bytes as usize, expected.len());
        assert_eq!(copied.last, Some(b'e'));
        worker.join().unwrap();
    }

    #[test]
    fn raw_copy_rejects_inconsistent_offsets_even_at_eof() {
        for data in [vec![], vec![1, 2], vec![0; 65537]] {
            let (mut client, worker) = rpc_pair(1, move |_| {
                Reply::Output(pb::OutputChunk {
                    next_offset: 1,
                    data: data.clone(),
                    eof: true,
                })
            });
            let mut output = Vec::new();
            assert!(copy_job_output(&mut client, 1, &mut output).is_err());
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
    fn shell_escapes_use_local_cwd_and_record_the_original_command_once() {
        let directory = tempfile::tempdir().unwrap();
        let selected = directory.path().canonicalize().unwrap();
        let history = directory.path().join("history");
        let result = directory.path().join("result with spaces");
        let pwd = directory.path().join("pwd");
        let command = format!(
            "!pwd > {}; printf '%s' 'some thing' | grep 'some thing' > {}",
            shlex::try_quote(pwd.to_str().unwrap()).unwrap(),
            shlex::try_quote(result.to_str().unwrap()).unwrap()
        );
        let expected = selected.clone();
        let (mut client, worker) = rpc_pair(1, move |op| {
            assert!(matches!(op, Op::LocalCwd(_)));
            Reply::LocalCwd(pb::Path {
                path: expected.to_str().unwrap().into(),
            })
        });
        assert!(dispatch_attached_command(
            &mut client,
            &command,
            history.to_str(),
            &mut Typeahead::default()
        )
        .unwrap());
        assert_eq!(fs::read_to_string(&result).unwrap(), "some thing\n");
        assert_eq!(
            fs::read_to_string(pwd).unwrap(),
            format!("{}\n", selected.display())
        );
        assert_eq!(fs::read_to_string(history).unwrap(), format!("{command}\n"));
        let mut server = worker.join().unwrap();
        server.set_nonblocking(true).unwrap();
        assert_eq!(
            std::io::Read::read(&mut server, &mut [0])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn password_settings_and_inline_credentials_are_never_written_to_history() {
        let root = tempfile::tempdir().unwrap();
        let history = root.path().join("history");
        fs::write(&history, "help\n").unwrap();
        let commands = [
            "set smb:password never-log-this",
            "open smb://user:never-log-this@host/share",
            "open smb://user:@host/share",
        ];
        let mut expected = commands.into_iter();
        let (mut client, worker) = rpc_pair(3, move |op| {
            let Op::Command(command) = op else {
                panic!("not a command")
            };
            assert_eq!(command.line, expected.next().unwrap());
            assert!(!format!("{command:?}").contains("never-log-this"));
            text_reply("declined")
        });
        for command in commands {
            assert!(!commands::should_record_history(command));
            assert!(dispatch_attached_command(
                &mut client,
                command,
                history.to_str(),
                &mut Typeahead::default()
            )
            .unwrap());
        }
        worker.join().unwrap();
        assert_eq!(fs::read_to_string(history).unwrap(), "help\n");
    }

    #[test]
    fn empty_command_does_not_send_anything_to_daemon() {
        let (stream, mut server) = UnixStream::pair().unwrap();
        let mut client = Connection::new(stream);
        for command in ["", " ", "\t  "] {
            assert!(dispatch_attached_command(
                &mut client,
                command,
                None,
                &mut Typeahead::default()
            )
            .unwrap());
        }
        server.set_nonblocking(true).unwrap();
        assert_eq!(
            std::io::Read::read(&mut server, &mut [0])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn passwords_use_a_separate_typed_request_not_command_text() {
        let (mut client, worker) = rpc_pair(1, |op| {
            let Op::ConnectPassword(credentials) = op else {
                panic!("not a password request")
            };
            assert_eq!(credentials.target, "server with spaces");
            assert_eq!(credentials.password, "secret \"quotes\" \\ slashes");
            assert!(!format!("{credentials:?}").contains("secret"));
            text_reply("accepted")
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
    fn command_semantic_errors_do_not_exit_the_interactive_client() {
        let mut first = true;
        let (mut client, worker) = rpc_pair(2, move |op| {
            assert!(matches!(op, Op::Command(_)));
            if std::mem::take(&mut first) {
                Reply::Error(pb::Error {
                    code: pb::ErrorCode::OperationFailed as i32,
                    message: "cannot change local directory".into(),
                })
            } else {
                text_reply("still attached")
            }
        });
        assert!(dispatch_attached_command(
            &mut client,
            "lcd missing",
            None,
            &mut Typeahead::default()
        )
        .unwrap());
        assert!(
            dispatch_attached_command(&mut client, "help", None, &mut Typeahead::default())
                .unwrap()
        );
        worker.join().unwrap();
    }

    #[test]
    fn startup_and_interactive_commands_share_dispatch() {
        let (mut client, worker) = rpc_pair(1, |op| {
            let Op::Command(command) = op else {
                panic!("not a command")
            };
            assert_eq!(command.line, "set s3:region eu-west-1");
            text_reply("region changed")
        });
        assert!(dispatch_attached_command(
            &mut client,
            "set s3:region eu-west-1",
            None,
            &mut Typeahead::default()
        )
        .unwrap());
        worker.join().unwrap();
    }
}
