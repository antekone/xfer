//! Real keyboard/terminal validation, including an installed OpenSSH SFTP server.
#[test]
fn ctrl_s_selector_preserves_editing_and_handles_local_remote_scrolling_and_cancel() {
    let Some(server) = [
        "/usr/lib/ssh/sftp-server",
        "/usr/lib/openssh/sftp-server",
        "/usr/libexec/sftp-server",
    ]
    .into_iter()
    .find(|path| std::path::Path::new(path).exists()) else {
        eprintln!("skipping selector PTY test: sftp-server not installed");
        return;
    };
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipping selector PTY test: python3 not installed");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new("python3")
        .args([
            "-c",
            include_str!("selector_pty.py"),
            env!("CARGO_BIN_EXE_xfer"),
            server,
        ])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "selector PTY failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
