//! Private, short-lived SSH askpass bridge. Passwords never appear in argv/env.
use crate::credentials::Password;
use anyhow::{bail, Result};
use std::{
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::PathBuf,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::UnixListener, process::Command, task::JoinHandle};
use zeroize::Zeroizing;

pub const SOCKET_ENV: &str = "XFER_SSH_ASKPASS_SOCKET";

pub(crate) struct Askpass {
    directory: tempfile::TempDir,
    task: JoinHandle<()>,
}
impl Askpass {
    pub fn new(password: Password) -> Result<Self> {
        if password.expose().len() > 16 * 1024 || password.expose().contains(['\n', '\r', '\0']) {
            bail!(
                "SSH passwords must be at most 16 KiB and cannot contain newline or NUL characters"
            );
        }
        let directory = tempfile::Builder::new()
            .prefix("xfer-askpass-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let path = directory.path().join("socket");
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                    stream.write_all(password.expose().as_bytes()).await?;
                    stream.write_all(b"\n").await
                })
                .await;
            }
        });
        Ok(Self { directory, task })
    }
    pub fn configure(&self, command: &mut Command) -> Result<()> {
        command
            .env("SSH_ASKPASS", std::env::current_exe()?)
            .env("SSH_ASKPASS_REQUIRE", "force")
            .env("DISPLAY", "xfer-askpass")
            .env(SOCKET_ENV, self.directory.path().join("socket"));
        Ok(())
    }
}
impl Drop for Askpass {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Invoked by OpenSSH using this executable as SSH_ASKPASS. Only answer password
/// prompts, never host-key confirmations, private-key passphrases, or MFA prompts.
pub fn main(prompt: &str) -> Result<()> {
    let prompt = prompt.to_ascii_lowercase();
    if !prompt.trim_end().ends_with("password:") || prompt.contains("passphrase") {
        bail!("unsupported SSH prompt; configure host keys and keys with ssh first");
    }
    let path = PathBuf::from(
        std::env::var_os(SOCKET_ENV).ok_or_else(|| anyhow::anyhow!("missing askpass socket"))?,
    );
    let mut socket = UnixStream::connect(path)?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut password = Zeroizing::new(Vec::new());
    (&mut socket).take(64 * 1024).read_to_end(&mut password)?;
    std::io::stdout().write_all(&password)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn askpass_is_private_and_removed_on_drop() {
        let askpass = Askpass::new(Password::new("private password".into())).unwrap();
        let directory = askpass.directory.path().to_owned();
        let path = directory.join("socket");
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut client = tokio::net::UnixStream::connect(path).await.unwrap();
        let mut value = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut client, &mut value)
            .await
            .unwrap();
        assert_eq!(value, "private password\n");
        drop(askpass);
        assert!(!directory.exists());
    }
    #[test]
    fn askpass_never_confirms_host_keys_or_supplies_key_passphrases() {
        assert!(main("Are you sure you want to continue connecting (yes/no)?").is_err());
        assert!(main("Enter passphrase for key '/home/user/.ssh/id_ed25519':").is_err());
        assert!(main("Enter passphrase for key '/home/password/id_ed25519':").is_err());
    }
}
