//! Modal, non-recursive filename selection. Directory data stays in the daemon's
//! private indexed snapshot; only a visible page and bounded selections live here.
use super::Connection;
use anyhow::Result;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute, queue,
    terminal::{
        self, BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate,
        EnterAlternateScreen, LeaveAlternateScreen,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
    time::Duration,
};
use xfer::ipc::pb;

const MAX_SELECTION_BYTES: usize = 64 * 1024;

pub(super) enum Selection {
    Accepted(String),
    Cancelled,
    Notice(String),
}

struct DirectoryJob<'a> {
    client: &'a mut Connection,
    id: Option<u64>,
    views: BTreeSet<u64>,
}
impl DirectoryJob<'_> {
    fn close_view(&mut self, id: u64) -> Result<()> {
        if self.views.remove(&id) {
            self.client.close_directory(id)?;
        }
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        let mut first = None;
        for id in std::mem::take(&mut self.views)
            .into_iter()
            .chain(self.id.take())
        {
            if let Err(error) = self.client.close_directory(id) {
                if first.is_none() {
                    first = Some(error);
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
impl Drop for DirectoryJob<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct Screen {
    previous: Vec<String>,
    size: Option<(u16, u16)>,
}
impl Screen {
    fn enter() -> Result<Self> {
        // Arm restoration before the first potentially partial terminal write.
        let screen = Self {
            previous: Vec::new(),
            size: None,
        };
        execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
        Ok(screen)
    }
    fn draw(&mut self, lines: Vec<String>, size: (u16, u16)) -> Result<()> {
        let mut frame = Vec::new();
        queue!(frame, BeginSynchronizedUpdate)?;
        if self.size != Some(size) {
            queue!(frame, Clear(ClearType::All))?;
            self.previous.clear();
        }
        let lines = lines
            .into_iter()
            .take(size.1 as usize)
            .map(|line| super::fit_status_line(&line, size.0.saturating_sub(1)))
            .collect::<Vec<_>>();
        for (row, line) in lines.iter().enumerate() {
            if self.previous.get(row) != Some(line) {
                queue!(
                    frame,
                    cursor::MoveTo(0, row as u16),
                    Clear(ClearType::CurrentLine)
                )?;
                frame.write_all(line.as_bytes())?;
            }
        }
        queue!(frame, EndSynchronizedUpdate)?;
        io::stdout().write_all(&frame)?;
        io::stdout().flush()?;
        self.previous = lines;
        self.size = Some(size);
        Ok(())
    }
}
impl Drop for Screen {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            EndSynchronizedUpdate,
            LeaveAlternateScreen,
            cursor::Show
        );
    }
}

fn operand(entry: &pb::DirectoryEntry) -> String {
    let mut name = entry.name.clone();
    // Explicit paths avoid option/home interpretation and basename glob
    // expansion in get/put; shell quoting separately protects Bash metacharacters.
    if name.starts_with(['-', '~']) || name.contains(['*', '?']) {
        name.insert_str(0, "./");
    }
    if entry.directory {
        name.push('/');
    }
    shlex::try_quote(&name)
        .expect("snapshot excludes NUL/control characters")
        .into_owned()
}
struct Picked {
    token: String,
    size: Option<u64>,
    directory: bool,
}

fn byte_size(bytes: u128) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut unit = 1u128;
    let mut label = "B";
    for next in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"] {
        unit *= 1024;
        label = next;
        if bytes / unit < 1024 {
            break;
        }
    }
    format!("{}.{} {label}", bytes / unit, (bytes % unit) * 10 / unit)
}
fn entry_size(entry: &pb::DirectoryEntry) -> String {
    if entry.directory {
        "<DIR>".into()
    } else {
        entry
            .size
            .map(|size| byte_size(size as u128))
            .unwrap_or_else(|| "?".into())
    }
}
fn next_sort(sort: pb::DirectorySort) -> pb::DirectorySort {
    use pb::DirectorySort::*;
    match sort {
        NameAsc => NameDesc,
        NameDesc => SizeDesc,
        SizeDesc => SizeAsc,
        SizeAsc => NameAsc,
    }
}
fn sort_label(sort: pb::DirectorySort) -> &'static str {
    use pb::DirectorySort::*;
    match sort {
        NameAsc => "name A-Z",
        NameDesc => "name Z-A",
        SizeDesc => "size largest",
        SizeAsc => "size smallest",
    }
}

#[derive(Default)]
struct State {
    focus: u64,
    selected: BTreeMap<u64, Picked>,
    bytes: usize,
    notice: String,
}
impl State {
    fn navigate(&mut self, key: KeyCode, total: u64, rows: usize) {
        let last = total.saturating_sub(1);
        self.focus = match key {
            KeyCode::Up | KeyCode::Char('k') => self.focus.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.focus.saturating_add(1).min(last),
            KeyCode::PageUp => self.focus.saturating_sub(rows as u64),
            KeyCode::PageDown => self.focus.saturating_add(rows as u64).min(last),
            KeyCode::Home => 0,
            KeyCode::End => last,
            _ => self.focus,
        };
    }
    fn toggle(&mut self, entry: &pb::DirectoryEntry) {
        if let Some(value) = self.selected.remove(&entry.index) {
            self.bytes -= value.token.len() + 1;
            self.notice.clear();
        } else {
            let value = operand(entry);
            if self.bytes.saturating_add(value.len() + 1) > MAX_SELECTION_BYTES {
                self.notice = "Selection limit: 64 KiB of quoted filenames".into();
            } else {
                self.bytes += value.len() + 1;
                self.selected.insert(
                    entry.index,
                    Picked {
                        token: value,
                        size: entry.size,
                        directory: entry.directory,
                    },
                );
                self.notice.clear();
            }
        }
    }
    fn summary(&self) -> String {
        let mut files = 0;
        let mut directories = 0;
        let mut unknown = 0;
        let mut bytes = 0u128;
        for picked in self.selected.values() {
            if picked.directory {
                directories += 1;
            } else {
                files += 1;
                if let Some(size) = picked.size {
                    bytes += size as u128;
                } else {
                    unknown += 1;
                }
            }
        }
        let mut text = format!("{files} files, {}", byte_size(bytes));
        if unknown != 0 {
            text.push_str(&format!(" known + {unknown} unknown"));
        }
        if directories != 0 {
            text.push_str(&format!(" + {directories} dirs"));
        }
        text
    }
    fn accept(self) -> Selection {
        if self.selected.is_empty() {
            Selection::Cancelled
        } else {
            Selection::Accepted(
                self.selected
                    .into_values()
                    .map(|picked| picked.token)
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        }
    }
}
const MAX_REGEXP_BYTES: usize = 4096;

struct FilterEdit {
    pattern: String,
    replace: bool,
    saved_focus: u64,
    apply_when_ready: bool,
}
struct FilterState {
    applied: String,
    applied_view: u64,
    visible: u64,
    visible_pattern: String,
    editing: Option<FilterEdit>,
    pending: Option<(u64, String, pb::DirectorySort)>,
    sort: pb::DirectorySort,
    visible_sort: pb::DirectorySort,
    applied_sort: pb::DirectorySort,
    error: String,
}
impl FilterState {
    fn new(base: u64) -> Self {
        Self {
            applied: ".*".into(),
            applied_view: base,
            visible: base,
            visible_pattern: ".*".into(),
            editing: None,
            pending: None,
            sort: pb::DirectorySort::NameAsc,
            visible_sort: pb::DirectorySort::NameAsc,
            applied_sort: pb::DirectorySort::NameAsc,
            error: String::new(),
        }
    }
    fn begin(&mut self, focus: u64) {
        self.editing = Some(FilterEdit {
            pattern: self.applied.clone(),
            replace: true,
            saved_focus: focus,
            apply_when_ready: false,
        });
        self.error.clear();
    }
    fn cancel_pending(&mut self, job: &mut DirectoryJob<'_>) -> Result<()> {
        if let Some((id, _, _)) = self.pending.take() {
            job.close_view(id)?;
        }
        Ok(())
    }
    fn update(&mut self, job: &mut DirectoryJob<'_>, base: u64) -> Result<bool> {
        let edit = self.editing.as_mut().expect("filter edit active");
        edit.apply_when_ready = false;
        let pattern = edit.pattern.clone();
        self.rebuild(job, base, pattern)
    }
    fn cycle_sort(&mut self, job: &mut DirectoryJob<'_>, base: u64) -> Result<bool> {
        self.sort = next_sort(self.sort);
        let changed = self.rebuild(job, base, self.applied.clone())?;
        if changed {
            self.commit_view(job)?;
        }
        Ok(changed)
    }
    fn rebuild(&mut self, job: &mut DirectoryJob<'_>, base: u64, pattern: String) -> Result<bool> {
        self.cancel_pending(job)?;
        self.error.clear();
        if pattern == self.visible_pattern && self.sort == self.visible_sort {
            return Ok(false);
        }
        let target = if pattern == ".*" && self.sort == pb::DirectorySort::NameAsc {
            Some(base)
        } else if pattern == self.applied && self.sort == self.applied_sort {
            Some(self.applied_view)
        } else {
            None
        };
        if let Some(target) = target {
            let old = self.visible;
            self.visible = target;
            self.visible_pattern = pattern;
            self.visible_sort = self.sort;
            if old != self.applied_view && old != self.visible {
                job.close_view(old)?;
            }
            return Ok(true);
        }
        match job
            .client
            .filter_directory_sorted(base, &pattern, self.sort)
        {
            Ok(id) => {
                job.views.insert(id);
                self.pending = Some((id, pattern, self.sort));
            }
            Err(error) if error.is::<xfer::ipc::RemoteError>() => self.error = error.to_string(),
            Err(error) => return Err(error),
        }
        Ok(false)
    }
    fn commit_view(&mut self, job: &mut DirectoryJob<'_>) -> Result<()> {
        let old = self.applied_view;
        self.applied_view = self.visible;
        self.applied_sort = self.visible_sort;
        if old != self.visible {
            job.close_view(old)?;
        }
        Ok(())
    }
    fn poll(&mut self, job: &mut DirectoryJob<'_>) -> Result<bool> {
        let Some((id, pattern, order)) = self.pending.clone() else {
            return Ok(false);
        };
        let status = job.client.job(id)?;
        match pb::JobState::try_from(status.state)? {
            pb::JobState::Running => Ok(false),
            pb::JobState::Completed => {
                self.pending = None;
                let old = self.visible;
                self.visible = id;
                self.visible_pattern = pattern;
                self.visible_sort = order;
                if old != self.applied_view {
                    job.close_view(old)?;
                }
                if self.editing.is_none() {
                    self.commit_view(job)?;
                } else if self
                    .editing
                    .as_ref()
                    .is_some_and(|edit| edit.apply_when_ready)
                {
                    self.apply(job)?;
                }
                Ok(true)
            }
            _ => {
                self.pending = None;
                job.close_view(id)?;
                self.error = match status.progress {
                    Some(pb::job::Progress::Indeterminate(value)) => value.message,
                    _ => "file filtering failed".into(),
                };
                if let Some(edit) = &mut self.editing {
                    edit.apply_when_ready = false;
                }
                Ok(false)
            }
        }
    }
    fn apply(&mut self, job: &mut DirectoryJob<'_>) -> Result<()> {
        if !self.error.is_empty() {
            return Ok(());
        }
        if self.pending.is_some() {
            self.editing.as_mut().unwrap().apply_when_ready = true;
            return Ok(());
        }
        let edit = self.editing.take().expect("filter edit active");
        self.applied = edit.pattern;
        self.commit_view(job)
    }
    fn revert(&mut self, job: &mut DirectoryJob<'_>) -> Result<u64> {
        self.cancel_pending(job)?;
        let edit = self.editing.take().expect("filter edit active");
        let old = self.visible;
        self.visible = self.applied_view;
        self.visible_pattern = self.applied.clone();
        self.sort = self.applied_sort;
        self.visible_sort = self.applied_sort;
        if old != self.applied_view {
            job.close_view(old)?;
        }
        self.error.clear();
        Ok(edit.saved_focus)
    }
    fn text(&mut self, text: &str) -> bool {
        let edit = self.editing.as_mut().expect("filter edit active");
        let mut pattern = if edit.replace {
            String::new()
        } else {
            edit.pattern.clone()
        };
        for ch in text.chars().filter(|ch| !ch.is_control()) {
            if pattern.len() + ch.len_utf8() > MAX_REGEXP_BYTES {
                self.error = "file filter exceeds 4096 bytes".into();
                return false;
            }
            pattern.push(ch);
        }
        let changed = pattern != edit.pattern;
        edit.pattern = pattern;
        edit.replace = false;
        changed
    }
    fn erase(&mut self, clear: bool) -> bool {
        let edit = self.editing.as_mut().expect("filter edit active");
        let previous = edit.pattern.clone();
        if clear || edit.replace {
            edit.pattern.clear();
        } else {
            edit.pattern.pop();
        }
        edit.replace = false;
        previous != edit.pattern
    }
    fn line(&self) -> String {
        if let Some(edit) = &self.editing {
            format!(
                "Filter /{}▏  [editing: Enter applies, Esc reverts, Ctrl-U clears]",
                edit.pattern
            )
        } else {
            format!(
                "Filter /{}  [/ edits regexp] · Sort: {} [s cycles]",
                self.applied,
                sort_label(self.visible_sort)
            )
        }
    }
}

fn navigation_key(code: KeyCode, modifiers: KeyModifiers) -> Option<KeyCode> {
    match (code, modifiers) {
        (KeyCode::Char('f'), KeyModifiers::CONTROL) => Some(KeyCode::PageDown),
        (KeyCode::Char('b'), KeyModifiers::CONTROL) => Some(KeyCode::PageUp),
        (code, KeyModifiers::NONE) => Some(code),
        _ => None,
    }
}

fn cancelled(code: KeyCode, modifiers: KeyModifiers) -> bool {
    code == KeyCode::Esc || (code == KeyCode::Char('c') && modifiers == KeyModifiers::CONTROL)
}
fn size() -> (u16, u16) {
    let (w, h) = terminal::size().unwrap_or((80, 24));
    (w, h.min(259))
}
fn rows(size: (u16, u16)) -> usize {
    size.1.saturating_sub(4).clamp(1, 256) as usize
}
fn contains(page: &pb::DirectoryPage, start: u64, focus: u64) -> bool {
    focus >= start && focus < page.next_offset
}

pub(super) fn select(
    client: &mut Connection,
    source: pb::DirectorySource,
    filter: pb::DirectoryFilter,
) -> Result<Selection> {
    let id = client.start_directory(source, filter)?;
    let mut job = DirectoryJob {
        client,
        id: Some(id),
        views: BTreeSet::new(),
    };
    let mut screen = Screen::enter()?;
    let result = run(&mut job, id, &mut screen, source);
    let closed = job.close();
    drop(screen);
    let result = result?;
    closed?;
    Ok(result)
}
fn run(
    job: &mut DirectoryJob<'_>,
    id: u64,
    screen: &mut Screen,
    source: pb::DirectorySource,
) -> Result<Selection> {
    let label = if source == pb::DirectorySource::Local {
        "local"
    } else {
        "remote"
    };
    loop {
        let status = job.client.job(id)?;
        match pb::JobState::try_from(status.state)? {
            pb::JobState::Completed => break,
            pb::JobState::Failed | pb::JobState::Cancelled => {
                let message = match status.progress {
                    Some(pb::job::Progress::Indeterminate(value)) => value.message,
                    _ => "directory listing failed".into(),
                };
                return Ok(Selection::Notice(message));
            }
            _ => {}
        }
        screen.draw(
            vec![
                format!("File selector — {label}"),
                "Reading current directory… Esc/Ctrl-C cancels".into(),
            ],
            size(),
        )?;
        if event::poll(Duration::from_millis(60))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Release && cancelled(key.code, key.modifiers) {
                    return Ok(Selection::Cancelled);
                }
            }
        }
    }
    let mut dimensions = size();
    let mut page = job.client.directory_page(id, 0, rows(dimensions) as u32)?;
    let mut loaded_view = id;
    let mut start = 0;
    let mut state = State::default();
    let mut filter = FilterState::new(id);
    loop {
        if filter.poll(job)? {
            state.focus = 0;
        }
        let current_size = size();
        if loaded_view != filter.visible
            || current_size != dimensions
            || (page.total > 0 && !contains(&page, start, state.focus))
        {
            dimensions = current_size;
            let old_total = page.total;
            let old_skipped = page.skipped;
            start = state
                .focus
                .saturating_sub(rows(dimensions).saturating_sub(1) as u64);
            page = job
                .client
                .directory_page(filter.visible, start, rows(dimensions) as u32)?;
            if page.total > 0 && !contains(&page, start, state.focus) {
                start = state.focus;
                page = job
                    .client
                    .directory_page(filter.visible, start, rows(dimensions) as u32)?;
            }
            if loaded_view == filter.visible
                && (page.total != old_total || page.skipped != old_skipped)
            {
                return job.client.unexpected();
            }
            loaded_view = filter.visible;
        }
        let mut lines = vec![
            format!("File selector — {label}: {}", page.path),
            "↑↓/j/k move  ^F/^B page  Home/End  Space toggle  s sort  / filter  Enter append  Esc cancel".into(),
            filter.line(),
        ];
        for (index, entry) in page.entries.iter().take(rows(dimensions)).enumerate() {
            let position = start + index as u64;
            lines.push(format!(
                "{} [{}] {:>10}  {}{}",
                if position == state.focus { ">" } else { " " },
                if state.selected.contains_key(&entry.index) {
                    "X"
                } else {
                    " "
                },
                entry_size(entry),
                entry.name,
                if entry.directory { "/" } else { "" }
            ));
        }
        if page.total == 0 {
            lines.push("(no matching entries)".into());
        }
        while lines.len() < dimensions.1.saturating_sub(1) as usize {
            lines.push(String::new());
        }
        let status = if !filter.error.is_empty() {
            filter.error.as_str()
        } else if filter.pending.is_some() {
            "sorting/filtering…"
        } else if !state.notice.is_empty() {
            state.notice.as_str()
        } else {
            ""
        };
        let footer = format!(
            "{} selected (incl hidden) · {} · {}/{} · {} skipped{}{}",
            state.selected.len(),
            state.summary(),
            if page.total == 0 { 0 } else { state.focus + 1 },
            page.total,
            page.skipped,
            if status.is_empty() { "" } else { " · " },
            status
        );
        if dimensions.1 >= 4 {
            lines.truncate(dimensions.1 as usize - 1);
            lines.push(footer);
        }
        screen.draw(lines, dimensions)?;
        if filter.pending.is_some() && !event::poll(Duration::from_millis(40))? {
            continue;
        }
        match event::read()? {
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                if filter.editing.is_some() {
                    if cancelled(key.code, key.modifiers) {
                        state.focus = filter.revert(job)?;
                        continue;
                    }
                    let changed = match (key.code, key.modifiers) {
                        (KeyCode::Enter, KeyModifiers::NONE) => {
                            filter.apply(job)?;
                            false
                        }
                        (KeyCode::Backspace, KeyModifiers::NONE) => filter.erase(false),
                        (KeyCode::Char('u'), KeyModifiers::CONTROL) => filter.erase(true),
                        (KeyCode::Char(ch), mods)
                            if !mods.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                                && !ch.is_control() =>
                        {
                            filter.text(&ch.to_string())
                        }
                        _ => false,
                    };
                    if changed && filter.update(job, id)? {
                        state.focus = 0;
                    }
                    continue;
                }
                if cancelled(key.code, key.modifiers) {
                    return Ok(Selection::Cancelled);
                }
                let Some(code) = navigation_key(key.code, key.modifiers) else {
                    continue;
                };
                match code {
                    KeyCode::Enter => return Ok(state.accept()),
                    KeyCode::Char('/') => filter.begin(state.focus),
                    KeyCode::Char('s') => {
                        if filter.cycle_sort(job, id)? {
                            state.focus = 0;
                        }
                    }
                    KeyCode::Char(' ') => {
                        if let Some(entry) =
                            page.entries.get(state.focus.saturating_sub(start) as usize)
                        {
                            state.toggle(entry);
                        }
                    }
                    code => state.navigate(code, page.total, rows(dimensions)),
                }
            }
            Event::Paste(text)
                if filter.editing.is_some() && filter.text(&text) && filter.update(job, id)? =>
            {
                state.focus = 0;
            }
            // Resize redraws the modal. Paste outside the filter never leaks
            // into the command behind the dialog or submits an unseen command.
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn file(name: &str) -> pb::DirectoryEntry {
        pb::DirectoryEntry {
            name: name.into(),
            directory: false,
            regular_file: true,
            index: 0,
            size: Some(0),
        }
    }
    #[test]
    fn toggles_preserve_directory_order_and_deselection() {
        let mut state = State {
            focus: 2,
            ..State::default()
        };
        state.toggle(&pb::DirectoryEntry {
            index: 2,
            ..file("日本語 'two'.txt")
        });
        state.focus = 0;
        state.toggle(&file("one.txt"));
        state.focus = 1;
        state.toggle(&pb::DirectoryEntry {
            index: 1,
            ..file("skip")
        });
        state.toggle(&pb::DirectoryEntry {
            index: 1,
            ..file("skip")
        });
        let Selection::Accepted(value) = state.accept() else {
            panic!()
        };
        assert_eq!(
            shlex::split(&value).unwrap(),
            ["one.txt", "日本語 'two'.txt"]
        );
    }
    #[test]
    fn selection_is_bounded_and_empty_acceptance_cancels() {
        assert!(matches!(State::default().accept(), Selection::Cancelled));
        let mut state = State::default();
        state.toggle(&file(&"x".repeat(MAX_SELECTION_BYTES)));
        assert!(state.selected.is_empty());
        assert!(!state.notice.is_empty());
        state.toggle(&file("a"));
        state.toggle(&file("a"));
        assert_eq!(state.bytes, 0);
    }
    #[test]
    fn operands_are_literal_and_quote_spaces_and_shell_syntax() {
        for name in [
            "-c",
            "~",
            "a*b",
            "question?",
            "$(touch pwned)",
            "a'b",
            "日本語.txt",
        ] {
            let token = operand(&file(name));
            let parsed = shlex::split(&token).unwrap();
            assert_eq!(parsed.len(), 1);
            let expected = if name.starts_with(['-', '~']) || name.contains(['*', '?']) {
                format!("./{name}")
            } else {
                name.into()
            };
            assert_eq!(parsed[0], expected);
        }
        let mut directory = file("folder");
        directory.directory = true;
        assert_eq!(operand(&directory), "folder/");
    }
    #[test]
    fn navigation_clamps_pages_boundaries_and_empty_directories() {
        let mut state = State::default();
        state.navigate(KeyCode::End, 100, 10);
        assert_eq!(state.focus, 99);
        state.navigate(KeyCode::PageUp, 100, 10);
        assert_eq!(state.focus, 89);
        state.navigate(KeyCode::PageDown, 100, 10);
        assert_eq!(state.focus, 99);
        state.navigate(KeyCode::Down, 100, 10);
        assert_eq!(state.focus, 99);
        state.navigate(KeyCode::Home, 100, 10);
        state.navigate(KeyCode::Up, 100, 10);
        assert_eq!(state.focus, 0);
        state.navigate(KeyCode::End, 0, 10);
        assert_eq!(state.focus, 0);
        for width in [0, 1, 10, 1000] {
            assert!((1..=256).contains(&rows((width, width))));
        }
    }
    #[test]
    fn directory_job_guard_closes_once_on_success_or_unwind() {
        use crate::{test_support::rpc_pair, Op, Reply};
        for explicit in [false, true] {
            let (mut client, worker) = rpc_pair(1, |op| {
                assert_eq!(op, Op::CloseDirectory(pb::JobId { id: 42 }));
                Reply::Ok(pb::Empty {})
            });
            {
                let mut guard = DirectoryJob {
                    client: &mut client,
                    id: Some(42),
                    views: BTreeSet::new(),
                };
                if explicit {
                    guard.close().unwrap();
                }
            }
            worker.join().unwrap();
        }
    }
    #[test]
    fn control_paging_and_filter_editing_are_unicode_safe_and_bounded() {
        assert_eq!(
            navigation_key(KeyCode::Char('f'), KeyModifiers::CONTROL),
            Some(KeyCode::PageDown)
        );
        assert_eq!(
            navigation_key(KeyCode::Char('b'), KeyModifiers::CONTROL),
            Some(KeyCode::PageUp)
        );
        assert_eq!(
            navigation_key(KeyCode::Down, KeyModifiers::NONE),
            Some(KeyCode::Down)
        );
        let mut filter = FilterState::new(10);
        assert_eq!(filter.applied, ".*");
        filter.begin(7);
        assert!(filter.text("^日本語"));
        assert_eq!(filter.editing.as_ref().unwrap().pattern, "^日本語");
        assert!(filter.erase(false));
        assert_eq!(filter.editing.as_ref().unwrap().pattern, "^日本");
        assert!(!filter.text(&"x".repeat(MAX_REGEXP_BYTES)));
        assert!(!filter.error.is_empty());
        assert_eq!(filter.editing.as_ref().unwrap().pattern, "^日本");
        assert!(filter.erase(true));
        assert_eq!(filter.editing.as_ref().unwrap().pattern, "");
    }
    #[test]
    fn preview_apply_and_revert_close_only_unneeded_views() {
        use crate::{test_support::rpc_pair, Op, Reply};
        for apply in [false, true] {
            let (mut client, worker) = rpc_pair(4, |op| match op {
                Op::FilterDirectory(request) => {
                    assert_eq!(request.job_id, 10);
                    assert_eq!(request.regexp, "foo");
                    Reply::DirectoryStarted(pb::JobId { id: 42 })
                }
                Op::Job(id) => {
                    assert_eq!(id.id, 42);
                    Reply::Job(pb::Job {
                        id: 42,
                        state: pb::JobState::Completed as i32,
                        ..Default::default()
                    })
                }
                Op::CloseDirectory(_) => Reply::Ok(pb::Empty {}),
                _ => panic!("unexpected operation"),
            });
            let mut job = DirectoryJob {
                client: &mut client,
                id: Some(10),
                views: BTreeSet::new(),
            };
            let mut filter = FilterState::new(10);
            filter.begin(7);
            filter.text("foo");
            filter.update(&mut job, 10).unwrap();
            assert_eq!(filter.applied, ".*");
            assert_eq!(filter.visible, 10);
            assert!(filter.poll(&mut job).unwrap());
            assert_eq!(filter.visible, 42);
            if apply {
                filter.apply(&mut job).unwrap();
                assert_eq!(filter.applied, "foo");
                assert_eq!(filter.applied_view, 42);
                assert!(job.views.contains(&42));
            } else {
                assert_eq!(filter.revert(&mut job).unwrap(), 7);
                assert_eq!(filter.applied, ".*");
                assert_eq!(filter.visible, 10);
                assert!(!job.views.contains(&42));
            }
            assert!(filter.editing.is_none());
            job.close().unwrap();
            worker.join().unwrap();
        }
    }
    #[test]
    fn invalid_regexp_cannot_be_applied_and_escape_restores_previous_filter() {
        use crate::{test_support::rpc_pair, Op, Reply};
        let (mut client, worker) = rpc_pair(2, |op| match op {
            Op::FilterDirectory(_) => Reply::Error(pb::Error {
                code: pb::ErrorCode::InvalidRequest as i32,
                message: "invalid regexp".into(),
            }),
            Op::CloseDirectory(_) => Reply::Ok(pb::Empty {}),
            _ => panic!("unexpected operation"),
        });
        let mut job = DirectoryJob {
            client: &mut client,
            id: Some(10),
            views: BTreeSet::new(),
        };
        let mut filter = FilterState::new(10);
        filter.begin(3);
        filter.text("[");
        filter.update(&mut job, 10).unwrap();
        assert!(!filter.error.is_empty());
        filter.apply(&mut job).unwrap();
        assert!(filter.editing.is_some());
        assert_eq!(filter.applied, ".*");
        assert_eq!(filter.revert(&mut job).unwrap(), 3);
        assert!(filter.error.is_empty());
        job.close().unwrap();
        worker.join().unwrap();
    }
    #[test]
    fn rapid_filter_edits_cancel_obsolete_jobs_before_restoring_the_applied_view() {
        use crate::{test_support::rpc_pair, Op, Reply};
        let mut next = 41;
        let (mut client, worker) = rpc_pair(5, move |op| match op {
            Op::FilterDirectory(_) => {
                next += 1;
                Reply::DirectoryStarted(pb::JobId { id: next })
            }
            Op::CloseDirectory(_) => Reply::Ok(pb::Empty {}),
            _ => panic!("unexpected operation"),
        });
        let mut job = DirectoryJob {
            client: &mut client,
            id: Some(10),
            views: BTreeSet::new(),
        };
        let mut filter = FilterState::new(10);
        filter.begin(0);
        filter.text("a");
        filter.update(&mut job, 10).unwrap();
        assert!(job.views.contains(&42));
        filter.text("b");
        filter.update(&mut job, 10).unwrap();
        assert!(!job.views.contains(&42));
        assert!(job.views.contains(&43));
        filter.revert(&mut job).unwrap();
        assert!(job.views.is_empty());
        job.close().unwrap();
        worker.join().unwrap();
    }
    #[test]
    fn selected_entries_keep_original_ids_across_filtered_positions() {
        let mut state = State::default();
        let entry = pb::DirectoryEntry {
            index: 2000,
            ..file("chosen")
        };
        state.toggle(&entry);
        state.focus = 17;
        state.toggle(&entry);
        assert!(state.selected.is_empty());
        state.toggle(&entry);
        assert!(state.selected.contains_key(&2000));
    }
    #[test]
    fn enter_during_preview_waits_for_that_filter_without_accepting_files() {
        use crate::{test_support::rpc_pair, Op, Reply};
        let (mut client, worker) = rpc_pair(4, |op| match op {
            Op::FilterDirectory(_) => Reply::DirectoryStarted(pb::JobId { id: 42 }),
            Op::Job(_) => Reply::Job(pb::Job {
                id: 42,
                state: pb::JobState::Completed as i32,
                ..Default::default()
            }),
            Op::CloseDirectory(_) => Reply::Ok(pb::Empty {}),
            _ => panic!("unexpected operation"),
        });
        let mut job = DirectoryJob {
            client: &mut client,
            id: Some(10),
            views: BTreeSet::new(),
        };
        let mut filter = FilterState::new(10);
        filter.begin(0);
        filter.text("foo");
        filter.update(&mut job, 10).unwrap();
        filter.apply(&mut job).unwrap();
        assert!(filter.editing.as_ref().unwrap().apply_when_ready);
        assert_eq!(filter.applied, ".*");
        assert!(filter.poll(&mut job).unwrap());
        assert!(filter.editing.is_none());
        assert_eq!(filter.applied, "foo");
        assert_eq!(filter.applied_view, 42);
        job.close().unwrap();
        worker.join().unwrap();
    }
    #[test]
    fn selection_summary_counts_files_and_sizes_without_overflow_or_directory_sizes() {
        let mut state = State::default();
        let first = pb::DirectoryEntry {
            index: 1,
            size: Some(u64::MAX),
            ..file("first")
        };
        let second = pb::DirectoryEntry {
            index: 2,
            size: Some(u64::MAX),
            ..file("second")
        };
        state.toggle(&first);
        state.toggle(&second);
        assert_eq!(
            state.summary(),
            format!("2 files, {}", byte_size(2 * u64::MAX as u128))
        );
        state.toggle(&pb::DirectoryEntry {
            index: 3,
            size: None,
            ..file("unknown")
        });
        state.toggle(&pb::DirectoryEntry {
            index: 4,
            directory: true,
            regular_file: false,
            size: Some(999),
            ..file("folder")
        });
        assert!(state.summary().contains("3 files"));
        assert!(state.summary().contains("known + 1 unknown + 1 dirs"));
        state.toggle(&first);
        assert_eq!(
            state.summary(),
            format!(
                "2 files, {} known + 1 unknown + 1 dirs",
                byte_size(u64::MAX as u128)
            )
        );
        assert_eq!(
            entry_size(&pb::DirectoryEntry {
                size: None,
                ..file("unknown")
            }),
            "?"
        );
        assert_eq!(byte_size(0), "0 B");
        assert_eq!(byte_size(1536), "1.5 KiB");
        assert!(byte_size(u128::MAX).ends_with("EiB"));
    }
    #[test]
    fn sorting_cycles_all_orders_without_losing_selections() {
        use crate::{test_support::rpc_pair, Op, Reply};
        let mut call = 0;
        let (mut client, worker) = rpc_pair(10, move |op| match op {
            Op::FilterDirectory(request) => {
                call += 1;
                assert_eq!(
                    request.sort,
                    [
                        pb::DirectorySort::NameDesc,
                        pb::DirectorySort::SizeDesc,
                        pb::DirectorySort::SizeAsc
                    ][call - 1] as i32
                );
                Reply::DirectoryStarted(pb::JobId {
                    id: 40 + call as u64,
                })
            }
            Op::Job(id) => Reply::Job(pb::Job {
                id: id.id,
                state: pb::JobState::Completed as i32,
                ..Default::default()
            }),
            Op::CloseDirectory(_) => Reply::Ok(pb::Empty {}),
            _ => panic!("unexpected operation"),
        });
        let mut job = DirectoryJob {
            client: &mut client,
            id: Some(10),
            views: BTreeSet::new(),
        };
        let mut filter = FilterState::new(10);
        let mut state = State::default();
        state.toggle(&pb::DirectoryEntry {
            index: 7,
            size: Some(123),
            ..file("chosen")
        });
        for order in [
            pb::DirectorySort::NameDesc,
            pb::DirectorySort::SizeDesc,
            pb::DirectorySort::SizeAsc,
        ] {
            filter.cycle_sort(&mut job, 10).unwrap();
            assert!(filter.poll(&mut job).unwrap());
            assert_eq!(filter.visible_sort, order);
            assert!(state.selected.contains_key(&7));
            assert_eq!(state.summary(), "1 files, 123 B");
        }
        assert!(filter.cycle_sort(&mut job, 10).unwrap());
        assert_eq!(filter.visible_sort, pb::DirectorySort::NameAsc);
        assert_eq!(filter.visible, 10);
        job.close().unwrap();
        worker.join().unwrap();
    }
}
