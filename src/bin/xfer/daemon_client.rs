//! Startup channel for the exact daemon spawned by this client. Discovery is
//! deliberately not involved: it is only for attaching to existing instances.
use super::Instance;
use anyhow::{bail, Context, Result};
use std::{
    env, io,
    io::{BufReader, Read},
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream, process::CommandExt},
    },
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_READY_BYTES: usize = 8192;

/// Kill and reap an unsuccessful startup, but leave a successfully connected
/// daemon independent of the client (including after a later detach).
struct StartingDaemon(Option<Child>);
impl Drop for StartingDaemon {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub(super) fn start() -> Result<(UnixStream, BufReader<UnixStream>)> {
    let executable = env::current_exe()?;
    let mut command = Command::new(&executable);

    command.arg("daemon");
    start_command(command, STARTUP_TIMEOUT)
        .with_context(|| format!("start daemon {}", executable.display()))
}

fn start_command(
    mut command: Command,
    timeout: Duration,
) -> Result<(UnixStream, BufReader<UnixStream>)> {
    let child = command
        .process_group(0) // terminal signals must not reach the daemon
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    // we expect that the daemon will print us the path to its communication
    // socket.

    let mut starting = StartingDaemon(Some(child));
    let child = starting.0.as_mut().unwrap();
    let deadline = Instant::now() + timeout;
    let instance = wait_for_ready(child, deadline)?;

    let mut stream = UnixStream::connect(&instance.socket).context("connect to spawned daemon")?;
    verify_peer(&stream, child.id())?;
    if let Some(status) = child.try_wait()? {
        bail!("daemon exited during startup ({status})");
    }

    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("timed out attaching to spawned daemon")?;
    let reader = super::claim_attachment(&instance, &mut stream, remaining)?;
    // Do not resolve the ID through discovery again: this exact, verified,
    // claimed connection is the attachment. Dropping Child does not kill it.
    starting.0.take();
    Ok((stream, reader))
}

fn wait_for_ready(child: &mut Child, deadline: Instant) -> Result<Instance> {
    let mut stdout = child.stdout.take().context("missing daemon startup pipe")?;
    // A blocking read (or a detached reader thread) could outlive the timeout
    // when a failed daemon or one of its descendants keeps stdout open.
    let fd = stdout.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error()).context("configure daemon startup pipe");
    }
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if Instant::now() >= deadline {
            bail!("timed out waiting for daemon readiness");
        }
        if let Some(status) = child.try_wait()? {
            bail!("daemon exited before readiness ({status})");
        }
        match stdout.read(&mut chunk) {
            Ok(0) => {
                if let Some(status) = child.try_wait()? {
                    bail!("daemon exited before readiness ({status})");
                }
                bail!("daemon closed startup pipe before readiness");
            }
            Ok(count) => {
                bytes.extend_from_slice(&chunk[..count]);
                if bytes.len() > MAX_READY_BYTES {
                    bail!("daemon readiness message is too large");
                }
                if let Some(end) = bytes.iter().position(|byte| *byte == b'\n') {
                    if end + 1 != bytes.len() {
                        bail!("unexpected data after daemon readiness message");
                    }
                    // Never echo malformed startup contents in diagnostics.
                    let instance: Instance = serde_json::from_slice(&bytes[..end])
                        .map_err(|_| anyhow::anyhow!("invalid daemon readiness message"))?;
                    if instance.pid != child.id()
                        || instance.id.is_empty()
                        || !instance
                            .id
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                        || !instance.socket.is_absolute()
                    {
                        bail!("invalid daemon readiness identity");
                    }
                    return Ok(instance);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(10)),
                );
            }
            Err(error) => return Err(error).context("read daemon readiness"),
        }
    }
}

// Compare the kernel-recorded listener PID, never an application-supplied PID
// or merely the peer UID (another daemon may run as the same user).
fn verify_peer(stream: &UnixStream, expected_pid: u32) -> Result<()> {
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
    ))]
    {
        #[cfg(target_os = "linux")]
        type PeerCredentials = libc::ucred;
        #[cfg(target_os = "freebsd")]
        type PeerCredentials = libc::xucred;
        #[cfg(target_os = "netbsd")]
        type PeerCredentials = libc::unpcbid;
        #[cfg(target_os = "openbsd")]
        type PeerCredentials = libc::sockpeercred;

        #[cfg(any(target_os = "linux", target_os = "openbsd"))]
        let (level, option) = (libc::SOL_SOCKET, libc::SO_PEERCRED);
        #[cfg(target_os = "freebsd")]
        let (level, option) = (libc::SOL_LOCAL, libc::LOCAL_PEERCRED);
        // NetBSD's <sys/un.h> defines SOL_LOCAL as 0; libc currently exports
        // LOCAL_PEEREID and unpcbid but not the SOL_LOCAL constant.
        #[cfg(target_os = "netbsd")]
        let (level, option) = (0, libc::LOCAL_PEEREID);

        let mut credentials = std::mem::MaybeUninit::<PeerCredentials>::uninit();
        let mut length = size_of::<PeerCredentials>() as libc::socklen_t;
        // SAFETY: the output buffer has the native ABI type/size corresponding
        // to this platform's socket option, and remains alive through the call.
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                level,
                option,
                credentials.as_mut_ptr().cast(),
                &mut length,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error()).context("identify spawned daemon peer");
        }
        if length as usize != size_of::<PeerCredentials>() {
            bail!("invalid daemon peer credentials");
        }
        // SAFETY: the successful syscall returned the complete native record;
        // each credential field is a plain integer or a C union of those types.
        let credentials = unsafe { credentials.assume_init() };
        #[cfg(any(target_os = "linux", target_os = "openbsd"))]
        let pid = credentials.pid;
        #[cfg(target_os = "netbsd")]
        let pid = credentials.unp_pid;
        #[cfg(target_os = "freebsd")]
        let pid = {
            if credentials.cr_version != libc::XUCRED_VERSION {
                bail!("unsupported daemon peer credentials version");
            }
            // SAFETY: LOCAL_PEERCRED supplies cr_pid in xucred's compatibility
            // union. Kernels without a usable PID fail the check below rather
            // than silently weakening verification to a UID comparison.
            unsafe { credentials.cr_pid__c_anonymous_union.cr_pid }
        };
        if pid <= 0 || pid as u32 != expected_pid {
            bail!("connected socket does not belong to the spawned daemon");
        }
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
    )))]
    let _ = (stream, expected_pid);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        command
    }

    fn readiness_error(script: &str, timeout: Duration) -> String {
        let mut child = shell(script).stdout(Stdio::piped()).spawn().unwrap();
        let result = wait_for_ready(&mut child, Instant::now() + timeout);
        let _ = child.kill();
        child.wait().unwrap();
        result.unwrap_err().to_string()
    }

    #[test]
    fn readiness_is_read_directly_from_child_stdout() {
        let mut child = shell("printf '{\"id\":\"own-instance\",\"pid\":%s,\"socket\":\"/exact/socket\"}\\n' \"$$\"; exec sleep 30")
            .stdout(Stdio::piped()).spawn().unwrap();
        let result = wait_for_ready(&mut child, Instant::now() + Duration::from_secs(2));
        let _ = child.kill();
        child.wait().unwrap();
        let instance = result.unwrap();
        assert_eq!(instance.pid, child.id());
        assert_eq!(instance.id, "own-instance");
        assert_eq!(instance.socket, std::path::Path::new("/exact/socket"));
    }

    #[test]
    fn readiness_timeout_does_not_wait_for_pipe_eof() {
        let start = Instant::now();
        let error = readiness_error("exec sleep 30", Duration::from_millis(100));
        assert!(error.contains("timed out"), "{error}");
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn early_exit_and_closed_startup_pipe_fail_immediately() {
        let start = Instant::now();
        let error = readiness_error("exit 23", Duration::from_secs(5));
        assert!(error.contains("before readiness"), "{error}");
        let error = readiness_error("exec 1>&-; exec sleep 30", Duration::from_secs(5));
        assert!(error.contains("before readiness"), "{error}");
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn malformed_partial_oversized_and_wrong_identity_messages_are_rejected() {
        for script in [
            "printf 'not-json-secret\\n'; exec sleep 30",
            "printf '{\"id\":\"unfinished\"'; exec 1>&-; exec sleep 30",
            "printf '{\"id\":\"wrong-pid\",\"pid\":0,\"socket\":\"/s\"}\\n'; exec sleep 30",
            "printf '{\"id\":\"relative-path\",\"pid\":%s,\"socket\":\"relative\"}\\n' \"$$\"; exec sleep 30",
            "printf '{\"id\":\"bad/id\",\"pid\":%s,\"socket\":\"/s\"}\\n' \"$$\"; exec sleep 30",
        ] {
            let error = readiness_error(script, Duration::from_secs(2));
            assert!(!error.contains("secret"));
            assert!(!error.contains("timed out"), "{error}");
        }
        let error = readiness_error("printf '%09000d' 0; exec sleep 30", Duration::from_secs(2));
        assert!(error.contains("too large"), "{error}");
    }

    #[test]
    fn failed_startups_are_killed_and_reaped() {
        let child = shell("exec sleep 30").spawn().unwrap();
        let pid = child.id();
        drop(StartingDaemon(Some(child)));
        assert_eq!(
            unsafe { libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
    ))]
    #[test]
    fn peer_credentials_identify_the_listener_and_reject_other_pids() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("s");
        let _listener = UnixListener::bind(&path).unwrap();
        let stream = UnixStream::connect(&path).unwrap();
        verify_peer(&stream, std::process::id()).unwrap();
        assert!(verify_peer(&stream, 0).is_err());
        assert!(verify_peer(&stream, std::process::id().wrapping_add(1)).is_err());
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
    ))]
    #[test]
    fn another_daemon_cannot_impersonate_the_child_through_its_announced_path() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("s");
        let listener = UnixListener::bind(&path).unwrap();
        // The child truthfully reports its own PID, but advertises our socket.
        let frame = serde_json::json!({"id": "other-daemon", "pid": "PID", "socket": path});
        let frame = frame.to_string().replace("\"PID\"", "%s");
        let script = format!("printf '{frame}\\n' \"$$\"; exec sleep 30");
        let error = start_command(shell(&script), Duration::from_secs(2)).unwrap_err();
        assert!(error.to_string().contains("does not belong"), "{error}");
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        // In particular, no ATTACH, CWD, rc command, or credential was sent.
        assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 0);
    }
}
