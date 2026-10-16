//! Command interface and the single registry used by daemon/client dispatch,
//! help, aliases and completion. Small handlers live here; larger ones in submodules.
use crate::{
    core::{Daemon, SessionInfo},
    credentials::Password,
};
use anyhow::{bail, Result};
use std::path::PathBuf;

pub(crate) mod bookmark;
pub(crate) mod cat;
pub(crate) mod cd;
pub(crate) mod connect;
pub(crate) mod get;
pub(crate) mod ls;
pub(crate) mod output;
pub(crate) mod put;
pub(crate) mod shell;
pub(crate) mod status;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionSite {
    Daemon,
    Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    None,
    Settings,
    RemotePath {
        request: &'static str,
        multiple_arguments: bool,
    },
}

/// Parsed xfer arguments alongside untouched source for job titles and Bash.
/// Shell escapes are recognized before shlex: Bash must parse its own syntax.
pub struct ParserContext<'a> {
    pub line: &'a str,
    name: String,
    args: Vec<String>,
    pub shell_source: Option<&'a str>,
}
impl<'a> ParserContext<'a> {
    pub fn parse(line: &'a str) -> Result<Self> {
        if let Some(source) = line.strip_prefix('!') {
            return Ok(Self {
                line,
                name: "!".into(),
                args: Vec::new(),
                shell_source: Some(source),
            });
        }
        let mut tokens = shlex::split(line)
            .ok_or_else(|| anyhow::anyhow!("invalid command quoting"))?
            .into_iter();
        Ok(Self {
            line,
            name: tokens.next().unwrap_or_default(),
            args: tokens.collect(),
            shell_source: None,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn arguments(&self) -> &[String] {
        &self.args
    }
    pub fn words(&self) -> impl Iterator<Item = &str> {
        self.args.iter().map(String::as_str)
    }
}

/// Minimal UI services needed by client commands. The IPC/terminal adapter stays
/// in the binary; command handlers need not depend on its concrete app type.
pub trait ClientApp {
    fn detach(&mut self) -> Result<()>;
    fn foreground(&mut self, id: crate::core::JobId) -> Result<()>;
}

pub enum CommandContext<'a> {
    Daemon {
        app: &'a mut Daemon,
        password: Option<Password>,
    },
    Client {
        app: &'a mut dyn ClientApp,
    },
}
impl CommandContext<'_> {
    pub fn site(&self) -> ExecutionSite {
        match self {
            Self::Daemon { .. } => ExecutionSite::Daemon,
            Self::Client { .. } => ExecutionSite::Client,
        }
    }
    pub fn daemon(&mut self) -> Result<&mut Daemon> {
        match self {
            Self::Daemon { app, .. } => Ok(app),
            Self::Client { .. } => bail!("this command must run in the daemon"),
        }
    }
    pub fn client(&mut self) -> Result<&mut dyn ClientApp> {
        match self {
            Self::Client { app } => Ok(*app),
            Self::Daemon { .. } => bail!("this command must run in the attached client"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandResult {
    Output(Vec<String>),
    Detach,
}
impl From<Vec<String>> for CommandResult {
    fn from(lines: Vec<String>) -> Self {
        Self::Output(lines)
    }
}

pub type Handler = fn(&mut CommandContext<'_>, &ParserContext<'_>) -> Result<CommandResult>;

pub trait Command: Sync {
    /// Canonical name first, followed by aliases.
    fn names(&self) -> &'static [&'static str];
    fn handler(&self) -> Handler;
    fn execute(
        &self,
        context: &mut CommandContext<'_>,
        parser: &ParserContext<'_>,
    ) -> Result<CommandResult> {
        if self.site() != context.site() {
            match self.site() {
                ExecutionSite::Daemon => bail!("this command must run in the daemon"),
                ExecutionSite::Client => bail!("this command must run in the attached client"),
            }
        }
        (self.handler())(context, parser)
    }
    fn matches(&self, parser: &ParserContext<'_>) -> bool {
        self.names().contains(&parser.name())
    }
    fn site(&self) -> ExecutionSite {
        ExecutionSite::Daemon
    }
    /// None hides protocol-internal commands from help and command completion.
    fn help(&self) -> Option<(&'static str, &'static str)> {
        None
    }
    fn completion(&self) -> Completion {
        Completion::None
    }
    /// Whether completed arguments before the active path are valid. Commands
    /// with options can accept flag prefixes without treating them as paths.
    fn accepts_path_completion(&self, arguments: &[String], _path: &str) -> bool {
        match self.completion() {
            Completion::RemotePath {
                multiple_arguments, ..
            } => multiple_arguments || arguments.is_empty(),
            _ => false,
        }
    }
    fn record_history(&self) -> bool {
        true
    }
}

/// Adding a command requires a handler/Command implementation and ONE entry here.
pub static COMMANDS: &[&dyn Command] = &[
    &HELP,
    &connect::Connect,
    &DISCONNECT,
    &cd::Cd,
    &PWD,
    &LCD,
    &ls::Ls,
    &get::Get,
    &put::Put,
    &cat::Cat,
    &bookmark::Bookmark,
    &status::Status,
    &JOBS,
    &CANCEL,
    &SHOW,
    &SET,
    &MIRROR,
    &FOREGROUND,
    &BACKGROUND,
    &shell::Shell,
    &DETACH,
    &output::READ_OUTPUT,
    &output::OUTPUT,
    &output::ACK_OUTPUT,
];

pub fn find(parser: &ParserContext<'_>) -> Option<&'static dyn Command> {
    COMMANDS
        .iter()
        .copied()
        .find(|command| command.matches(parser))
}

pub fn completion_for(name: &str) -> Completion {
    COMMANDS
        .iter()
        .find(|command| command.names().contains(&name))
        .map(|command| command.completion())
        .unwrap_or(Completion::None)
}

pub fn accepts_path_completion(name: &str, arguments: &[String], path: &str) -> bool {
    COMMANDS
        .iter()
        .find(|command| command.names().contains(&name))
        .is_some_and(|command| command.accepts_path_completion(arguments, path))
}

pub fn visible_names() -> impl Iterator<Item = &'static str> {
    let mut names = COMMANDS
        .iter()
        .filter(|command| command.help().is_some())
        .flat_map(|command| command.names().iter().copied())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.into_iter()
}

pub fn dispatch_daemon(app: &mut Daemon, line: &str) -> Vec<String> {
    let parser = match ParserContext::parse(line) {
        Ok(parser) => parser,
        Err(error) => return vec![error.to_string()],
    };
    run_daemon(app, &parser, None)
}

/// Passwords are out-of-band context, never parser source, history or job titles.
pub fn dispatch_connect(app: &mut Daemon, target: &str, password: Option<Password>) -> Vec<String> {
    let parser = ParserContext {
        line: "connect",
        name: "connect".into(),
        args: vec![target.into()],
        shell_source: None,
    };
    run_daemon(app, &parser, password)
}

fn run_daemon(
    app: &mut Daemon,
    parser: &ParserContext<'_>,
    password: Option<Password>,
) -> Vec<String> {
    let Some(command) = find(parser) else {
        return vec!["unknown daemon command".into()];
    };
    match command.execute(&mut CommandContext::Daemon { app, password }, parser) {
        Ok(CommandResult::Output(lines)) => lines,
        Ok(CommandResult::Detach) => unreachable!("daemon command cannot detach a client"),
        Err(error) => vec![error.to_string()],
    }
}

/// Lightweight command implementation for grouped handlers.
struct SmallCommand {
    names: &'static [&'static str],
    help: Option<(&'static str, &'static str)>,
    handler: Handler,
    site: ExecutionSite,
    completion: Completion,
    history: bool,
}
impl Command for SmallCommand {
    fn names(&self) -> &'static [&'static str] {
        self.names
    }
    fn handler(&self) -> Handler {
        self.handler
    }
    fn help(&self) -> Option<(&'static str, &'static str)> {
        self.help
    }
    fn site(&self) -> ExecutionSite {
        self.site
    }
    fn completion(&self) -> Completion {
        self.completion
    }
    fn record_history(&self) -> bool {
        self.history
    }
}

fn help(_context: &mut CommandContext<'_>, _parser: &ParserContext<'_>) -> Result<CommandResult> {
    let mut entries = Vec::new();
    for command in COMMANDS {
        if let Some((usage, description)) = command.help() {
            entries.push((command.names()[0], format!("  {usage:<20} {description}")));
            for &alias in &command.names()[1..] {
                entries.push((
                    alias,
                    format!("  {alias:<20} alias for {}", command.names()[0]),
                ));
            }
        }
    }
    entries.sort_unstable_by_key(|(name, _)| *name);
    let mut lines = vec!["Commands:".into()];
    lines.extend(entries.into_iter().map(|(_, line)| line));
    Ok(lines.into())
}

fn foreground(
    context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> Result<CommandResult> {
    let [id] = parser.arguments() else {
        return Ok(vec!["usage: fg <job-id>".into()].into());
    };
    let Ok(id) = id.parse() else {
        return Ok(vec!["usage: fg <job-id>".into()].into());
    };
    context.client()?.foreground(id)?;
    Ok(Vec::new().into())
}
fn background(
    _context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> Result<CommandResult> {
    if !parser.arguments().is_empty() {
        return Ok(vec!["usage: bg".into()].into());
    }
    Ok(vec!["all jobs are already in the background".into()].into())
}
fn detach(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    if !parser.arguments().is_empty() {
        return Ok(vec!["usage: detach".into()].into());
    }
    context.client()?.detach()?;
    Ok(CommandResult::Detach)
}

const fn small(
    names: &'static [&'static str],
    help: Option<(&'static str, &'static str)>,
    handler: Handler,
) -> SmallCommand {
    SmallCommand {
        names,
        help,
        handler,
        site: ExecutionSite::Daemon,
        completion: Completion::None,
        history: true,
    }
}
static HELP: SmallCommand = small(&["help"], Some(("help", "show available commands")), help);
static DISCONNECT: SmallCommand = small(
    &["disconnect", "close"],
    Some(("disconnect", "release the active backend connection")),
    disconnect,
);
static PWD: SmallCommand = small(
    &["pwd"],
    Some(("pwd", "print backend working directory")),
    pwd,
);
static LCD: SmallCommand = small(
    &["lcd"],
    Some((
        "lcd [directory]",
        "change daemon local download directory (default: home)",
    )),
    lcd,
);
static JOBS: SmallCommand = small(&["jobs"], Some(("jobs", "list daemon jobs")), jobs);
static CANCEL: SmallCommand = small(
    &["cancel"],
    Some(("cancel <job-id>", "cancel a job")),
    cancel,
);
static SHOW: SmallCommand = SmallCommand {
    completion: Completion::Settings,
    ..small(
        &["show"],
        Some(("show [setting]", "show daemon settings")),
        show,
    )
};
static SET: SmallCommand = SmallCommand {
    completion: Completion::Settings,
    ..small(
        &["set"],
        Some(("set <name> <value>", "update a daemon setting")),
        set,
    )
};
static MIRROR: SmallCommand = small(&["mirror"], Some(("mirror", "not implemented yet")), mirror);
static FOREGROUND: SmallCommand = SmallCommand {
    site: ExecutionSite::Client,
    history: false,
    ..small(
        &["fg"],
        Some(("fg <job-id>", "foreground a daemon job")),
        foreground,
    )
};
static BACKGROUND: SmallCommand = SmallCommand {
    site: ExecutionSite::Client,
    history: false,
    ..small(
        &["bg"],
        Some(("bg", "leave jobs in the background")),
        background,
    )
};
static DETACH: SmallCommand = SmallCommand {
    site: ExecutionSite::Client,
    history: false,
    ..small(
        &["detach", "exit"],
        Some(("detach", "detach; daemon remains only while jobs run")),
        detach,
    )
};

fn disconnect_lines(app: &mut Daemon, _parser: &ParserContext<'_>) -> Vec<String> {
    let connection = app.connection.clone();
    let session = app.session.clone();
    let id = app.jobs.start_operation("disconnect".into(), async move {
        let previous = {
            let mut active = connection.lock().await;
            let previous = active.take();
            *session.lock().expect("session state poisoned") = SessionInfo::disconnected();
            previous
        };
        match previous {
            Some(value) => {
                let token = value.protocol();
                value.disconnect().await?;
                Ok(format!("disconnected {token}"))
            }
            None => Ok("not connected".into()),
        }
    });
    vec![format!("JOB {id}")]
}

fn disconnect(
    context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> Result<CommandResult> {
    Ok(disconnect_lines(context.daemon()?, parser).into())
}

fn pwd_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    if words.next().is_some() {
        return vec!["usage: pwd".into()];
    }
    let connection = app.connection.clone();
    let id = app.jobs.start_operation("pwd".into(), async move {
        let active = connection.lock().await;
        active
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("not connected; use connect first"))?
            .pwd()
    });
    vec![format!("JOB {id}")]
}

fn pwd(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(pwd_lines(context.daemon()?, parser).into())
}

fn lcd(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    let path = match parser.arguments() {
        [] => "~",
        [path] if !path.is_empty() => path.as_str(),
        _ => return Ok(vec!["usage: lcd [directory]".into()].into()),
    };
    let home = || {
        std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("cannot resolve local home directory: HOME is not set"))
    };
    let path = if path == "~" {
        home()?
    } else if let Some(relative) = path.strip_prefix("~/") {
        home()?.join(relative.trim_start_matches('/'))
    } else {
        PathBuf::from(path)
    };
    let app = context.daemon()?;
    let path = app.local_directory.join(path);
    app.set_local_directory(path)
        .map_err(|error| anyhow::anyhow!("cannot change local directory: {error}"))?;
    Ok(vec![format!("Local CWD: {}", app.local_directory.display())].into())
}

fn jobs_lines(app: &mut Daemon, _parser: &ParserContext<'_>) -> Vec<String> {
    app.jobs
        .jobs
        .values()
        .map(|job| format!("[{}] {:?} {}", job.id, job.state, job.title))
        .collect()
}

fn jobs(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(jobs_lines(context.daemon()?, parser).into())
}

fn cancel_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match words.next().and_then(|value| value.parse().ok()) {
        Some(id) => {
            app.jobs.cancel(id);
            vec![format!("cancel requested for job {id}")]
        }
        None => vec!["usage: cancel <job-id>".into()],
    }
}

fn cancel(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(cancel_lines(context.daemon()?, parser).into())
}

fn show_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match words.next() {
        Some(name) => match app.settings.values.get(name) {
            Some(setting) => vec![format!(
                "{name}: {} — {}",
                setting.value.display(),
                setting.doc
            )],
            None => vec![format!("unknown setting: {name}")],
        },
        None => {
            let mut names = app.settings.values.keys().collect::<Vec<_>>();
            names.sort_unstable();
            names
                .into_iter()
                .map(|name| {
                    let setting = &app.settings.values[name];
                    format!("{name}: {} — {}", setting.value.display(), setting.doc)
                })
                .collect()
        }
    }
}

fn show(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(show_lines(context.daemon()?, parser).into())
}

fn set_lines(app: &mut Daemon, parser: &ParserContext<'_>) -> Vec<String> {
    let mut words = parser.words();
    match (words.next(), words.next()) {
        (Some(name), Some(value)) => match app.settings.set(name, value) {
            Ok(()) => vec![format!("{name} set to {value}")],
            Err(error) => vec![error.to_string()],
        },
        _ => vec!["usage: set <name> <value>".into()],
    }
}

fn set(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(set_lines(context.daemon()?, parser).into())
}

fn mirror_lines(_app: &mut Daemon, _parser: &ParserContext<'_>) -> Vec<String> {
    vec!["mirror is not implemented yet".into()]
}

fn mirror(context: &mut CommandContext<'_>, parser: &ParserContext<'_>) -> Result<CommandResult> {
    Ok(mirror_lines(context.daemon()?, parser).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn registry_names_are_unique_aliases_match_and_hidden_commands_stay_hidden() {
        let mut names = BTreeSet::new();
        for command in COMMANDS {
            assert!(!command.names().is_empty());
            for name in command.names() {
                assert!(names.insert(*name), "duplicate command {name}");
                let parser = ParserContext::parse(name).unwrap();
                assert!(std::ptr::eq(find(&parser).unwrap(), *command), "{name}");
            }
        }
        let visible = visible_names().collect::<BTreeSet<_>>();
        for alias in [
            "connect",
            "open",
            "disconnect",
            "close",
            "detach",
            "exit",
            "fg",
            "bg",
            "!",
        ] {
            assert!(visible.contains(alias), "missing {alias}");
        }
        for internal in ["read-output", "output", "ack-output"] {
            assert!(!visible.contains(internal));
            assert!(find(&ParserContext::parse(internal).unwrap()).is_some());
        }
    }

    #[test]
    fn parser_preserves_quoted_arguments_and_does_not_parse_bash_source() {
        let parser = ParserContext::parse("  cd 'photo album' ").unwrap();
        assert_eq!(parser.name(), "cd");
        assert_eq!(parser.arguments(), &["photo album"]);
        assert_eq!(parser.line, "  cd 'photo album' ");
        assert!(ParserContext::parse("cd 'unfinished").is_err());
        let source = "!printf '%s\\n' \"$HOME\" | grep 'unterminated";
        let parser = ParserContext::parse(source).unwrap();
        assert_eq!(parser.name(), "!");
        assert_eq!(parser.shell_source, Some(&source[1..]));
        assert_eq!(find(&parser).unwrap().site(), ExecutionSite::Client);
        assert!(ParserContext::parse("show '!not a shell'")
            .unwrap()
            .shell_source
            .is_none());
    }

    #[test]
    fn registry_controls_help_and_argument_completion() {
        let mut app = Daemon::bootstrap().unwrap();
        let help = app.command("help").join("\n");
        for command in COMMANDS {
            if let Some((usage, _)) = command.help() {
                assert!(help.contains(usage), "missing {usage}");
            }
        }
        assert!(help.contains("alias for connect"));
        assert!(!help.contains("read-output"));
        assert_eq!(completion_for("show"), Completion::Settings);
        assert_eq!(completion_for("set"), Completion::Settings);
        assert_eq!(
            completion_for("cd"),
            Completion::RemotePath {
                request: "COMPLETE_CD",
                multiple_arguments: false
            }
        );
        assert_eq!(
            completion_for("ls"),
            Completion::RemotePath {
                request: "COMPLETE_LS",
                multiple_arguments: false
            }
        );
        assert_eq!(
            completion_for("get"),
            Completion::RemotePath {
                request: "COMPLETE_GET",
                multiple_arguments: true
            }
        );
        assert_eq!(completion_for("cat"), completion_for("get"));
        assert_eq!(completion_for("bogus"), Completion::None);
    }

    #[test]
    fn help_and_command_completion_sort_canonical_names_and_aliases_together() {
        let names = visible_names().collect::<Vec<_>>();
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(names.contains(&"lcd"));
        let mut app = Daemon::bootstrap().unwrap();
        let help = app.command("help");
        assert_eq!(help[0], "Commands:");
        let displayed = help[1..]
            .iter()
            .map(|line| line.split_whitespace().next().unwrap())
            .map(|name| if name == "!<command>" { "!" } else { name })
            .collect::<Vec<_>>();
        assert_eq!(displayed, names);
    }

    #[test]
    fn lcd_changes_only_the_daemon_local_directory_and_resolves_paths() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let album = base.join("photo album");
        std::fs::create_dir(&album).unwrap();
        std::os::unix::fs::symlink("photo album", base.join("link")).unwrap();
        let process_directory = std::env::current_dir().unwrap();
        let mut app = Daemon::bootstrap().unwrap();
        app.set_local_directory(base.clone()).unwrap();
        app.session.lock().unwrap().cwd = Some("/remote/photos/".into());
        assert_eq!(
            app.command("lcd 'photo album'"),
            vec![format!("Local CWD: {}", album.display())]
        );
        assert_eq!(app.local_directory, album);
        assert_eq!(
            app.command("lcd .."),
            vec![format!("Local CWD: {}", base.display())]
        );
        assert_eq!(
            app.command("lcd link"),
            vec![format!("Local CWD: {}", album.display())]
        );
        let absolute = format!("lcd {}", shlex::try_quote(base.to_str().unwrap()).unwrap());
        assert_eq!(
            app.command(&absolute),
            vec![format!("Local CWD: {}", base.display())]
        );
        let status = app.command("status");
        assert!(status.contains(&format!("Local CWD: {}", base.display())));
        assert!(status.contains(&"CWD: /remote/photos/".into()));
        assert_eq!(std::env::current_dir().unwrap(), process_directory);
        assert!(app.jobs.jobs.is_empty());
    }

    #[test]
    fn lcd_errors_do_not_change_the_local_directory() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file"), b"not a directory").unwrap();
        let mut app = Daemon::bootstrap().unwrap();
        app.set_local_directory(root.path().to_owned()).unwrap();
        let original = app.local_directory.clone();
        for input in ["lcd a b", "lcd ''"] {
            assert_eq!(app.command(input), vec!["usage: lcd [directory]"]);
            assert_eq!(app.local_directory, original);
        }
        assert!(app.command("lcd missing")[0].starts_with("cannot change local directory:"));
        assert_eq!(app.local_directory, original);
        assert!(app.command("lcd file")[0].contains("not a directory"));
        assert_eq!(app.local_directory, original);
        assert!(app.jobs.jobs.is_empty());
    }

    #[derive(Default)]
    struct MockClient {
        detached: bool,
        foreground: Vec<u64>,
    }
    impl ClientApp for MockClient {
        fn detach(&mut self) -> Result<()> {
            self.detached = true;
            Ok(())
        }
        fn foreground(&mut self, id: u64) -> Result<()> {
            self.foreground.push(id);
            Ok(())
        }
    }
    fn client_command(client: &mut MockClient, line: &str) -> CommandResult {
        let parser = ParserContext::parse(line).unwrap();
        find(&parser)
            .unwrap()
            .execute(&mut CommandContext::Client { app: client }, &parser)
            .unwrap()
    }

    #[test]
    fn client_commands_use_the_app_adapter_and_aliases() {
        let mut client = MockClient::default();
        assert_eq!(
            client_command(&mut client, "fg 42"),
            CommandResult::Output(Vec::new())
        );
        assert_eq!(client.foreground, vec![42]);
        assert_eq!(
            client_command(&mut client, "fg nonsense"),
            CommandResult::Output(vec!["usage: fg <job-id>".into()])
        );
        assert_eq!(
            client_command(&mut client, "bg"),
            CommandResult::Output(vec!["all jobs are already in the background".into()])
        );
        assert_eq!(client_command(&mut client, "exit"), CommandResult::Detach);
        assert!(client.detached);
        for name in ["fg", "bg", "detach", "exit"] {
            assert!(!find(&ParserContext::parse(name).unwrap())
                .unwrap()
                .record_history());
        }
    }

    #[test]
    fn execution_sites_prevent_running_local_commands_in_daemon() {
        let mut app = Daemon::bootstrap().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("must-not-exist");
        let shell = format!(
            "!touch {}",
            shlex::try_quote(path.to_str().unwrap()).unwrap()
        );
        assert_eq!(
            app.command(&shell),
            vec!["this command must run in the attached client"]
        );
        assert!(!path.exists());
        assert!(app.jobs.jobs.is_empty());
        let parser = ParserContext::parse("set s3:region example").unwrap();
        let mut client = MockClient::default();
        assert!(find(&parser)
            .unwrap()
            .execute(&mut CommandContext::Client { app: &mut client }, &parser)
            .is_err());
    }

    #[test]
    fn grouped_handlers_preserve_errors_and_settings_behavior() {
        let mut app = Daemon::bootstrap().unwrap();
        assert_eq!(app.command("unknown"), vec!["unknown daemon command"]);
        assert_eq!(
            app.command("show 'unfinished"),
            vec!["invalid command quoting"]
        );
        assert_eq!(
            app.command("set s3:region 'test region'"),
            vec!["s3:region set to test region"]
        );
        assert!(app.command("show s3:region")[0].starts_with("s3:region: test region"));
        for (input, usage) in [
            ("cd a b", "usage: cd [directory]"),
            ("ls a b", "usage: ls [-FlarS] [--] [path]"),
            ("get", "usage: get [-c] [--] <filenames...>"),
            ("pwd extra", "usage: pwd"),
        ] {
            assert_eq!(app.command(input), vec![usage]);
        }
        assert!(app.jobs.jobs.is_empty());
    }
}
