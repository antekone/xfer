//! Transport-independent backend and connection interfaces.
use crate::{bookmarks::Bookmark, core::Settings, error::NonRetryableError, listing::ListingSink};
use anyhow::{bail, Result};
use async_trait::async_trait;

pub use crate::{s3::S3, sftp::Sftp, smb::Smb, webdav::Webdav};

#[derive(Clone)]
pub struct ConnectOptions {
    pub target: String,
    pub bookmark: Option<Bookmark>,
    pub settings: Settings,
    pub password: Option<crate::credentials::Password>,
}

#[async_trait]
pub trait Connection: Send + Sync {
    fn protocol(&self) -> &'static str;
    async fn list(&self, _path: &str, _output: &mut dyn ListingSink) -> Result<u64> {
        bail!("listing is not implemented for {}", self.protocol())
    }
    async fn stat_file(&self, _path: &str) -> Result<crate::transfer::RemoteFile> {
        Err(NonRetryableError(format!(
            "file reads are not implemented for {}",
            self.protocol()
        ))
        .into())
    }
    async fn upload(&self, _local: &std::path::Path, _path: &str, _overwrite: bool) -> Result<()> {
        Err(NonRetryableError(format!(
            "file uploads are not implemented for {}",
            self.protocol()
        ))
        .into())
    }
    async fn download(
        &self,
        _path: &str,
        _offset: u64,
        _expected: &crate::transfer::RemoteFile,
        _sink: &mut dyn crate::transfer::DownloadSink,
    ) -> Result<()> {
        Err(NonRetryableError(format!(
            "file reads are not implemented for {}",
            self.protocol()
        ))
        .into())
    }
    /// Canonicalize a directory against this connection's current location.
    /// None selects the connection's initial directory (shell-style `cd`).
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        Ok(path.unwrap_or("/").to_owned())
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        self.resolve_directory(Some(path))
    }
    fn pwd(&self) -> Result<String> {
        bail!("pwd is not implemented for {}", self.protocol())
    }
    async fn change_directory(&mut self, _path: &str) -> Result<()> {
        Err(NonRetryableError(format!("cd is not implemented for {}", self.protocol())).into())
    }
    /// Create a remote directory; parents allows existing directories and creates
    /// missing ancestors. Implementations must not change the working directory.
    async fn create_directory(&self, _path: &str, _parents: bool) -> Result<()> {
        Err(NonRetryableError(format!("mkdir is not implemented for {}", self.protocol())).into())
    }
    async fn complete_directories(&self, _input: &str) -> Result<crate::cache::PathCompletion> {
        bail!(
            "directory completion is not available for {}",
            self.protocol()
        )
    }
    async fn complete_paths(&self, _input: &str) -> Result<crate::cache::PathCompletion> {
        bail!("path completion is not available for {}", self.protocol())
    }
    async fn disconnect(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait Backend: Send + Sync {
    fn protocol(&self) -> &'static str;
    fn register_settings(&self, settings: &mut Settings);
    fn initialize(&self, _settings: &Settings) -> Result<()> {
        Ok(())
    }
    async fn connect(&self, _options: ConnectOptions) -> Result<Box<dyn Connection>> {
        bail!("{} backend is not implemented yet", self.protocol())
    }
}
