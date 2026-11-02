//! Command-line interface for the xfer binary.
//!
//! The askpass bridge is intentionally not part of this parser: SSH selects it
//! through the environment and passes its prompt as a free-form argv element.
use clap::{Parser, Subcommand};

/// lftp-style transfer client with S3, SFTP, SMB and WebDAV backends.
#[derive(Debug, Parser)]
#[command(name = "xfer", version, args_conflicts_with_subcommands = true)]
pub struct Cli {
    /// Execute commands from a file non-interactively, then exit
    #[arg(short = 'f', long = "file", value_name = "FILE")]
    file: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the daemon server loop (spawned by the client, not interactive)
    #[command(hide = true)]
    Daemon,
    /// Attach this terminal to a running daemon; without ID, the only ready one
    Attach {
        /// Daemon instance ID prefix, as printed by a bare `xfer attach`
        id: Option<String>,
    },
}

/// What this invocation of the binary should do.
#[derive(Debug, PartialEq, Eq)]
pub enum Invocation {
    /// Start a daemon in the background and attach to it.
    StartAndAttach,
    /// Run only the daemon server loop.
    Daemon,
    /// Attach to an existing daemon, optionally selecting it by ID prefix.
    Attach(Option<String>),
    /// Start a fresh daemon and execute a command file.
    Script(std::path::PathBuf),
}

/// Parse the process arguments, exiting with usage/help errors like other clap
/// programs (stderr message plus status 2) before any daemon work starts.
pub fn parse() -> Invocation {
    let Cli { command, file } = Cli::parse();
    if let Some(file) = file {
        return Invocation::Script(file);
    }
    match command {
        Some(Command::Daemon) => Invocation::Daemon,
        Some(Command::Attach { id }) => Invocation::Attach(id),
        None => Invocation::StartAndAttach,
    }
}
