//! Client-side Bash escapes with live terminal I/O, never daemon jobs.
use anyhow::{Context, Result};
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitStatus, Stdio};

fn bash_command(source: &str) -> Command {
    let mut command = Command::new("bash");
    // Pass the source intact: Bash owns quoting, expansion, redirects, pipelines.
    command
        .arg("-c")
        .arg(source)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command
}

struct TerminalGuard {
    was_raw: bool,
    termios: Option<libc::termios>,
    signals: Vec<(libc::c_int, libc::sigaction)>,
}
impl TerminalGuard {
    fn new() -> std::io::Result<Self> {
        let was_raw = crossterm::terminal::is_raw_mode_enabled()?;
        crossterm::terminal::disable_raw_mode()?;
        let mut guard = Self {
            was_raw,
            termios: None,
            signals: Vec::new(),
        };
        // External programs may change tty settings, including on abnormal exit.
        // SAFETY: tcgetattr initializes termios on success; stdin may be non-TTY.
        unsafe {
            let mut saved = std::mem::MaybeUninit::uninit();
            if libc::tcgetattr(libc::STDIN_FILENO, saved.as_mut_ptr()) == 0 {
                guard.termios = Some(saved.assume_init());
            }
        }
        // Bash and xfer share the foreground terminal process group. Ignore
        // interrupts in the waiting client, but reset them in the child below.
        for signal in [libc::SIGINT, libc::SIGQUIT] {
            // SAFETY: initialized sigaction structures with valid signal numbers.
            unsafe {
                let mut ignore: libc::sigaction = std::mem::zeroed();
                ignore.sa_sigaction = libc::SIG_IGN;
                libc::sigemptyset(&mut ignore.sa_mask);
                let mut saved = std::mem::MaybeUninit::uninit();
                if libc::sigaction(signal, &ignore, saved.as_mut_ptr()) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                guard.signals.push((signal, saved.assume_init()));
            }
        }
        Ok(guard)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Some(termios) = &self.termios {
            // SAFETY: restore the tty settings captured from this same fd.
            unsafe {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, termios);
            }
        }
        if self.was_raw {
            let _ = crossterm::terminal::enable_raw_mode();
        }
        for (signal, saved) in self.signals.iter().rev() {
            // SAFETY: restore the exact prior dispositions saved by sigaction.
            unsafe {
                libc::sigaction(*signal, saved, std::ptr::null_mut());
            }
        }
    }
}

pub fn run(source: &str) -> Result<ExitStatus> {
    let _terminal = TerminalGuard::new().context("prepare terminal for local Bash command")?;
    let mut command = bash_command(source);
    // SAFETY: only async-signal-safe libc operations run between fork and exec.
    unsafe {
        command.pre_exec(|| {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = libc::SIG_DFL;
            libc::sigemptyset(&mut action.sa_mask);
            for signal in [libc::SIGINT, libc::SIGQUIT] {
                if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    command.status().context("run installed bash")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bash_handles_pipelines_quoting_expansion_and_both_output_streams() {
        let output = bash_command("printf '%s\\n' \"$XFER_SHELL_TEST\" 'other' | grep 'some thing'; printf 'problem\\n' >&2; exit 7")
            .env("XFER_SHELL_TEST", "some thing")
            .stdout(Stdio::piped()).stderr(Stdio::piped()).output().unwrap();
        assert_eq!(output.stdout, b"some thing\n");
        assert_eq!(output.stderr, b"problem\n");
        assert_eq!(output.status.code(), Some(7));
    }
    #[test]
    fn bash_runs_in_the_callers_directory_and_cd_does_not_change_it() {
        let directory = tempfile::tempdir().unwrap();
        let output = bash_command("pwd; cd /; pwd")
            .current_dir(directory.path())
            .stdout(Stdio::piped())
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("{}\n/\n", directory.path().display())
        );
    }
}
