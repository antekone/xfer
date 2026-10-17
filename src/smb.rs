//! Native async SMB2/3 through smb2. No mounts, shell commands, or blocking SMB
//! worker threads. Failed/cancelled sessions are replaced lazily by outer retries.
mod client;
use crate::{
    backend::{Backend, ConnectOptions, Connection},
    core::{SettingValue, Settings},
    credentials::Password,
    error::NonRetryableError,
    listing::{Content, DirectoryInfo, FileInfo, ListingSink},
    transfer::{DownloadSink, RemoteFile},
};
use anyhow::Result;
use async_trait::async_trait;
use client::Client as SmbClient;
use futures_util::{stream::FuturesOrdered, StreamExt};
use percent_encoding::percent_decode_str;
use smb2::{ClientConfig, ErrorKind, FileAttributes, Tree, WriteBehind};
use std::{fs::File, future::Future, io::Read, path::Path, time::Duration};
use tokio::sync::{Mutex, MutexGuard};

const CHUNK: usize = 64 * 1024;
pub struct Smb;

pub(crate) fn validate_target(
    value: &str,
    settings: &Settings,
    bookmark: Option<&crate::bookmarks::Bookmark>,
) -> Result<()> {
    let mut target = Target::parse(value, settings.text("smb:domain").unwrap_or(""))?;
    target.credentials(settings, bookmark, None)?;
    Options::read(settings)?;
    Ok(())
}
fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Location {
    share: Option<String>,
    path: String,
}
impl Location {
    fn display(&self) -> String {
        match &self.share {
            None => "/".into(),
            Some(share) => format!("/{share}/{}", self.path),
        }
    }
    fn resolve(&self, path: &str) -> Result<Self> {
        if path.contains('\0') {
            return Err(permanent("NUL is not allowed in SMB paths"));
        }
        let base = self.display();
        let mut parts = if path.starts_with('/') {
            Vec::new()
        } else {
            base.split('/')
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
        };
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop();
                }
                _ => parts.push(part),
            }
        }
        let Some(share) = parts.first() else {
            return Ok(Self::default());
        };
        if share.contains('\\') || share.contains(':') {
            return Err(permanent("invalid SMB share name"));
        }
        Ok(Self {
            share: Some((*share).into()),
            path: if parts.len() > 1 {
                format!("{}/", parts[1..].join("/"))
            } else {
                String::new()
            },
        })
    }
    fn key(&self) -> &str {
        self.path.trim_end_matches('/')
    }
}
fn directory_input(path: &str) -> bool {
    path.is_empty() || path.ends_with('/') || matches!(path.rsplit('/').next(), Some("." | ".."))
}

#[derive(Debug)]
struct Target {
    address: String,
    username: String,
    domain: String,
    home: Location,
    user_present: bool,
    domain_present: bool,
    password: Option<Password>,
}
impl Target {
    fn parse(value: &str, default_domain: &str) -> Result<Self> {
        let url = url::Url::parse(value).map_err(|_| permanent("invalid SMB URL"))?;
        if url.scheme() != "smb" || url.query().is_some() || url.fragment().is_some() {
            return Err(permanent(
                "expected smb://[DOMAIN;user[:password]@]host[:port][/share/path]",
            ));
        }
        let host = match url.host() {
            Some(url::Host::Ipv6(host)) => format!("[{host}]"),
            Some(host) => host.to_string(),
            None => return Err(permanent("SMB URL requires a host")),
        };
        if host.chars().any(|ch| ch.is_control() || ch.is_whitespace()) || url.port() == Some(0) {
            return Err(permanent("invalid SMB host or port"));
        }
        let decode = |value: &str| -> Result<String> {
            let value = percent_decode_str(value)
                .decode_utf8()
                .map_err(|_| permanent("SMB URLs must be UTF-8"))?
                .into_owned();
            if value.chars().any(char::is_control) {
                return Err(permanent("control characters are not allowed in SMB URLs"));
            }
            Ok(value)
        };
        let user = decode(url.username())?;
        let identity = user.split_once(';').or_else(|| user.split_once('\\'));
        let domain_present = identity.is_some();
        let (domain, username) = match identity {
            Some((domain, user)) => (domain.to_owned(), user.to_owned()),
            None => (default_domain.to_owned(), user),
        };
        let authority = value
            .split_once("://")
            .unwrap()
            .1
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("");
        let userinfo = authority.rsplit_once('@').map(|(info, _)| info);
        // Inspect the original authority too: URL normalization can erase an
        // explicitly empty user/password, which must override configured defaults.
        let password = userinfo
            .and_then(|info| info.split_once(':'))
            .map(|(_, password)| {
                percent_decode_str(password)
                    .decode_utf8()
                    .map(|value| Password::new(value.into_owned()))
                    .map_err(|_| permanent("SMB URL password must be valid UTF-8"))
            })
            .transpose()?;
        let mut path = String::new();
        for component in url.path().split('/') {
            let component = decode(component)?;
            if component.contains('/') {
                return Err(permanent(
                    "encoded slashes are not allowed in SMB URL components",
                ));
            }
            path.push('/');
            path.push_str(&component);
        }
        Ok(Self {
            address: format!("{host}:{}", url.port().unwrap_or(445)),
            username,
            domain,
            home: Location::default().resolve(&path)?,
            user_present: userinfo.is_some(),
            domain_present,
            password,
        })
    }
    fn credentials(
        &mut self,
        settings: &Settings,
        bookmark: Option<&crate::bookmarks::Bookmark>,
        interactive: Option<&Password>,
    ) -> Result<Option<Password>> {
        if let Some(user) = bookmark.and_then(|bookmark| bookmark.user.as_ref()) {
            self.username = user.clone();
        } else if !self.user_present {
            self.username = settings.text("smb:user").unwrap_or("").into();
        }
        if let Some(domain) = bookmark.and_then(|bookmark| bookmark.domain.as_ref()) {
            self.domain = domain.clone();
        } else if !self.domain_present {
            self.domain = settings.text("smb:domain").unwrap_or("").into();
        }
        let password = interactive
            .cloned()
            .or_else(|| bookmark.and_then(|bookmark| bookmark.password.clone()))
            .or_else(|| self.password.take())
            .or_else(|| settings.secret("smb:password").cloned());
        if self.username.is_empty() {
            // An explicit empty user is a NULL session, not the local OS user
            // or a named Guest login. Never send inherited domain/password data.
            self.domain.clear();
            self.password = None;
            return Ok(None);
        }
        if self.username.chars().any(char::is_control) || self.domain.chars().any(char::is_control)
        {
            return Err(permanent(
                "control characters are not allowed in SMB user/domain",
            ));
        }
        self.password = None;
        Ok(password)
    }
}

#[derive(Clone, Debug)]
struct Options {
    connect_timeout: Duration,
    io_timeout: Duration,
    window: usize,
    dfs: bool,
    encryption: bool,
}
impl Options {
    fn read(settings: &Settings) -> Result<Self> {
        let number = |name: &str, max: i64| -> Result<u64> {
            match settings.values.get(name).map(|setting| &setting.value) {
                Some(SettingValue::Int(value)) if (1..=max).contains(value) => Ok(*value as u64),
                _ => Err(permanent(format!("{name} must be between 1 and {max}"))),
            }
        };
        if settings.bool("smb:dfs") && settings.bool("smb:require-encryption") {
            return Err(permanent("smb:dfs and smb:require-encryption cannot be combined: referral encryption cannot be enforced on every target"));
        }
        Ok(Self {
            connect_timeout: Duration::from_secs(number("smb:connect-timeout", 300)?),
            io_timeout: Duration::from_secs(number("smb:operation-timeout", 300)?),
            window: number("smb:max-credits", 128)? as usize,
            dfs: settings.bool("smb:dfs"),
            encryption: settings.bool("smb:require-encryption"),
        })
    }
}
#[async_trait]
impl Backend for Smb {
    fn protocol(&self) -> &'static str {
        "smb"
    }
    fn register_settings(&self, settings: &mut Settings) {
        for (name, value, description) in [
            (
                "smb:max-credits",
                32,
                "Maximum in-flight 64 KiB transfer chunks (1..128; reconnect to apply)",
            ),
            (
                "smb:connect-timeout",
                10,
                "SMB connection/login timeout in seconds",
            ),
            (
                "smb:operation-timeout",
                30,
                "SMB operation timeout in seconds",
            ),
        ] {
            settings.register(name, SettingValue::Int(value), description);
        }
        settings.register(
            "smb:user",
            SettingValue::Text(String::new()),
            "Default SMB user; empty means anonymous, never the OS username",
        );
        settings.register(
            "smb:password",
            SettingValue::Secret(None),
            "Default SMB password (redacted; prefer per-bookmark credentials)",
        );
        settings.register(
            "smb:domain",
            SettingValue::Text(String::new()),
            "Default NTLM domain (or use DOMAIN;user in the URL)",
        );
        settings.register(
            "smb:dfs",
            SettingValue::Bool(false),
            "Allow DFS referrals to other servers, forwarding credentials (reconnect to apply)",
        );
        settings.register(
            "smb:require-encryption",
            SettingValue::Bool(false),
            "Refuse SMB shares without active SMB3 encryption (reconnect to apply)",
        );
    }
    fn initialize(&self, settings: &Settings) -> Result<()> {
        Options::read(settings).map(|_| ())
    }
    async fn connect(&self, options: ConnectOptions) -> Result<Box<dyn Connection>> {
        let mut target = Target::parse(
            options
                .bookmark
                .as_ref()
                .map(|bookmark| bookmark.url.as_str())
                .unwrap_or(&options.target),
            options.settings.text("smb:domain").unwrap_or(""),
        )?;
        let password = target.credentials(
            &options.settings,
            options.bookmark.as_ref(),
            options.password.as_ref(),
        )?;
        let parameters = Options::read(&options.settings)?;
        let home = target.home.clone();
        let recipe = Recipe {
            target,
            parameters,
            password,
        };
        let mut session = recipe.establish().await?;
        if home.share.is_some() {
            let info = session.stat(&home).await?;
            if !info.is_directory {
                return Err(permanent("SMB URL does not select a directory"));
            }
        } else {
            session.shares().await?;
        }
        Ok(Box::new(SmbConnection {
            recipe,
            session: Mutex::new(Some(session)),
            cwd: home.clone(),
            home,
        }))
    }
}

struct Recipe {
    target: Target,
    parameters: Options,
    password: Option<Password>,
}
impl Recipe {
    async fn establish(&self) -> Result<Session> {
        let config = ClientConfig {
            addr: self.target.address.clone(),
            username: self.target.username.clone(),
            domain: self.target.domain.clone(),
            password: self
                .password
                .as_ref()
                .map(|password| password.expose().to_owned())
                .unwrap_or_default(),
            timeout: self.parameters.connect_timeout,
            response_timeout: Some(self.parameters.io_timeout),
            // xfer owns retry/backoff and file-integrity checks; don't nest retries.
            auto_reconnect: false,
            dfs_enabled: self.parameters.dfs,
            ..Default::default()
        };
        let client = call(self.parameters.connect_timeout, SmbClient::connect(config)).await?;
        let mut session = Session {
            client,
            share: None,
            options: self.parameters.clone(),
        };
        if self.parameters.encryption {
            if self.target.username.is_empty() || self.target.username.eq_ignore_ascii_case("guest")
            {
                return Err(permanent(
                    "SMB encryption requires an authenticated, non-guest account",
                ));
            }
            session.enable_encryption()?;
        }
        Ok(session)
    }
}
struct Session {
    client: SmbClient,
    share: Option<(String, Tree)>,
    options: Options,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.client.connection().mark_dead();
    }
}
impl Session {
    fn enable_encryption(&mut self) -> Result<()> {
        use smb2::{crypto::encryption::Cipher, types::Dialect};
        if self.client.connection().should_encrypt() {
            return Ok(());
        }
        let params = self
            .client
            .params()
            .ok_or_else(|| permanent("SMB negotiation is incomplete"))?;
        let cipher = match params.dialect {
            Dialect::Smb2_0_2 | Dialect::Smb2_1 => {
                return Err(permanent("SMB encryption requires SMB3"))
            }
            Dialect::Smb3_1_1 => params
                .cipher
                .ok_or_else(|| permanent("server did not negotiate SMB3 encryption"))?,
            _ => params.cipher.unwrap_or(Cipher::Aes128Ccm),
        };
        let keys = self
            .client
            .session()
            .ok_or_else(|| permanent("anonymous SMB sessions cannot be encrypted"))?;
        let enc = keys
            .encryption_key
            .clone()
            .ok_or_else(|| permanent("SMB encryption key is unavailable"))?;
        let dec = keys
            .decryption_key
            .clone()
            .ok_or_else(|| permanent("SMB decryption key is unavailable"))?;
        // Encrypt TREE_CONNECT as well as subsequent data. Some servers refuse
        // an unencrypted TREE_CONNECT to an encryption-required share.
        self.client
            .connection_mut()
            .activate_encryption(enc, dec, cipher);
        Ok(())
    }
    async fn select(&mut self, name: &str) -> Result<()> {
        if self
            .share
            .as_ref()
            .is_some_and(|(current, _)| current.eq_ignore_ascii_case(name))
        {
            return Ok(());
        }
        if let Some((_, tree)) = self.share.take() {
            call(self.options.io_timeout, self.client.disconnect_share(&tree)).await?;
        }
        let tree = call(self.options.io_timeout, self.client.connect_share(name)).await?;
        if (self.options.encryption || tree.encrypt_data)
            && !self.client.connection().should_encrypt()
        {
            self.client.connection().mark_dead();
            return Err(permanent(
                "SMB share requires encryption but no encrypted session is active",
            ));
        }
        self.share = Some((name.into(), tree));
        Ok(())
    }
    fn parts(&mut self) -> (&mut SmbClient, &mut Tree) {
        (
            &mut self.client,
            &mut self.share.as_mut().expect("selected SMB share").1,
        )
    }
    async fn stat(&mut self, path: &Location) -> Result<smb2::FileInfo> {
        self.select(
            path.share
                .as_deref()
                .ok_or_else(|| permanent("select an SMB share first"))?,
        )
        .await?;
        let timeout = self.options.io_timeout;
        let (client, tree) = self.parts();
        call(timeout, client.stat(tree, path.key())).await
    }
    async fn shares(&mut self) -> Result<Vec<smb2::ShareInfo>> {
        if self.options.encryption && !self.client.connection().should_encrypt() {
            return Err(permanent(
                "select an encrypted SMB share explicitly when smb:require-encryption is enabled",
            ));
        }
        call(self.options.io_timeout, self.client.list_shares()).await
    }
}

#[derive(Debug)]
struct TransportError(String);
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TransportError {}
#[derive(Debug)]
struct InvalidSession(String);
impl std::fmt::Display for InvalidSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
fn convert(error: smb2::Error) -> anyhow::Error {
    let kind = error.kind();
    // Never expose library authentication messages, server text, or config:
    // these can carry credentials. Kind/status are sufficient and bounded.
    let message = format!(
        "SMB operation failed ({kind:?}{})",
        error
            .status()
            .map(|status| format!(", {status}"))
            .unwrap_or_default()
    );
    if error.is_retryable()
        || matches!(
            kind,
            ErrorKind::Io
                | ErrorKind::ConnectionLost
                | ErrorKind::SessionExpired
                | ErrorKind::TimedOut
        )
    {
        TransportError(message).into()
    } else if matches!(
        kind,
        ErrorKind::SignatureInvalid
            | ErrorKind::InvalidData
            | ErrorKind::GuestSessionRefused
            | ErrorKind::AuthRequired
            | ErrorKind::SigningRequired
    ) {
        permanent(message.clone()).context(InvalidSession(message))
    } else {
        permanent(message)
    }
}
async fn raw<T>(
    timeout: Duration,
    future: impl Future<Output = smb2::Result<T>>,
) -> smb2::Result<T> {
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or(Err(smb2::Error::Timeout))
}
async fn call<T>(timeout: Duration, future: impl Future<Output = smb2::Result<T>>) -> Result<T> {
    raw(timeout, future).await.map_err(convert)
}

struct SmbConnection {
    recipe: Recipe,
    session: Mutex<Option<Session>>,
    cwd: Location,
    home: Location,
}
struct Lease<'a> {
    session: Option<Session>,
    slot: MutexGuard<'a, Option<Session>>,
}
impl Lease<'_> {
    fn get(&mut self) -> &mut Session {
        self.session.as_mut().unwrap()
    }
    fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        let bad = result
            .as_ref()
            .is_err_and(|error| error.is::<TransportError>() || error.is::<InvalidSession>())
            || self.session.as_ref().unwrap().client.is_disconnected();
        if !bad {
            *self.slot = self.session.take();
        }
        // Otherwise Session::drop closes the socket. A cancelled lease also drops
        // its owned session, reclaiming remote handles that could not be closed.
        result
    }
}
impl SmbConnection {
    async fn acquire(&self) -> Result<Lease<'_>> {
        let mut slot = self.session.lock().await;
        let session = match slot.take() {
            Some(session) => session,
            None => self.recipe.establish().await?,
        };
        Ok(Lease {
            session: Some(session),
            slot,
        })
    }
}

fn record(name: String, size: u64, modified: smb2::pack::FileTime, directory: bool) -> Content {
    let modified = modified.to_system_time().map(chrono::DateTime::from);
    if directory {
        Content::Directory(DirectoryInfo {
            name,
            modified,
            size: Some(size),
            unix: None,
        })
    } else {
        Content::File(FileInfo {
            name,
            modified,
            size: Some(size),
            unix: None,
        })
    }
}
fn remote_file(info: &smb2::FileInfo) -> Result<RemoteFile> {
    if info.is_directory || info.attributes.0 & FileAttributes::REPARSE_POINT != 0 {
        return Err(permanent(
            "expected a regular SMB file, not a directory or reparse point",
        ));
    }
    Ok(RemoteFile {
        size: info.size,
        // CreationTime is not stable on Samba: separate opens may report
        // native birth time or synthesize it from mtime. LastWrite/ChangeTime
        // are the useful content/metadata revision checks, in 100 ns ticks.
        revision: Some(format!("{}:{}", info.modified.0, info.changed.0)),
    })
}
fn unchanged(info: &smb2::FileInfo, expected: &RemoteFile) -> Result<()> {
    let actual = remote_file(info)?;
    if actual.size != expected.size || actual.revision != expected.revision {
        return Err(permanent(format!(
            "remote SMB file changed; partial file was retained (expected {} bytes {:?}, got {} bytes {:?})",
            expected.size, expected.revision, actual.size, actual.revision,
        )));
    }
    Ok(())
}
fn valid_name(name: &str) -> Result<()> {
    if matches!(name, "" | "." | "..") || name.contains('/') || name.contains('\0') {
        return Err(permanent("SMB server returned an invalid name"));
    }
    Ok(())
}

#[async_trait]
impl Connection for SmbConnection {
    fn protocol(&self) -> &'static str {
        "smb"
    }
    fn pwd(&self) -> Result<String> {
        Ok(self.cwd.display())
    }
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        Ok(match path {
            None => self.home.display(),
            Some(path) => self.cwd.resolve(path)?.display(),
        })
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        let target = self.cwd.resolve(path)?;
        let value = target.display();
        Ok(if !target.path.is_empty() && !directory_input(path) {
            value.trim_end_matches('/').into()
        } else {
            value
        })
    }
    async fn change_directory(&mut self, path: &str) -> Result<()> {
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = async {
            if target.share.is_none() {
                lease.get().shares().await?;
            } else if !lease.get().stat(&target).await?.is_directory {
                return Err(permanent("not an SMB directory"));
            }
            Ok(())
        }
        .await;
        lease.finish(result)?;
        self.cwd = target;
        Ok(())
    }
    async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = async { remote_file(&lease.get().stat(&target).await?) }.await;
        lease.finish(result)
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = list_on(lease.get(), &target, directory_input(path), output).await;
        lease.finish(result)
    }
    async fn download(
        &self,
        path: &str,
        offset: u64,
        expected: &RemoteFile,
        sink: &mut dyn DownloadSink,
    ) -> Result<()> {
        if offset > expected.size {
            return Err(permanent("resume offset exceeds remote size"));
        }
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = download_on(lease.get(), &target, offset, expected, sink).await;
        lease.finish(result)
    }
    async fn upload(&self, local: &Path, path: &str, overwrite: bool) -> Result<()> {
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = upload_on(lease.get(), local, &target, overwrite).await;
        lease.finish(result)
    }
    async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        let target = self.cwd.resolve(path)?;
        let mut lease = self.acquire().await?;
        let result = mkdir_on(lease.get(), &target, parents).await;
        lease.finish(result)
    }
    async fn disconnect(self: Box<Self>) -> Result<()> {
        let session = self.session.lock().await.take();
        drop(session); // closes transport and releases library credential copies
        Ok(())
    }
}

async fn list_on(
    session: &mut Session,
    target: &Location,
    directory: bool,
    output: &mut dyn ListingSink,
) -> Result<u64> {
    if target.share.is_none() {
        let mut count = 0;
        for share in session.shares().await? {
            if share.share_type & 0xffff != 0 {
                continue;
            }
            valid_name(&share.name)?;
            if share.name.contains('\\') {
                return Err(permanent("SMB server returned an invalid share name"));
            }
            output.emit(Content::Directory(DirectoryInfo {
                name: share.name,
                modified: None,
                size: None,
                unix: None,
            }))?;
            count += 1;
        }
        return Ok(count);
    }
    let metadata = session.stat(target).await?;
    if !metadata.is_directory {
        if directory {
            return Err(permanent("not an SMB directory"));
        }
        output.emit_file(record(
            target.key().rsplit('/').next().unwrap().into(),
            metadata.size,
            metadata.modified,
            false,
        ))?;
        return Ok(1);
    }
    let timeout = session.options.io_timeout;
    let (client, tree) = session.parts();
    let mut reader = call(timeout, client.open_directory_reader(tree, target.key())).await?;
    let result: Result<u64> = async {
        let mut count = 0;
        while let Some(batch) = call(timeout, reader.next_batch()).await? {
            for entry in batch {
                if matches!(entry.name.as_str(), "." | "..") {
                    continue;
                }
                valid_name(&entry.name)?;
                // Don't recurse through descendant junctions/symlinks in du.
                let directory =
                    entry.is_directory && entry.attributes.0 & FileAttributes::REPARSE_POINT == 0;
                output.emit(record(entry.name, entry.size, entry.modified, directory))?;
                count += 1;
            }
        }
        Ok(count)
    }
    .await;
    if result
        .as_ref()
        .is_err_and(|error| error.is::<TransportError>() || error.is::<InvalidSession>())
    {
        return result; // lease teardown will reclaim the handle on a dead session
    }
    let closed = call(timeout, reader.close()).await;
    if closed.is_err() {
        session.client.connection().mark_dead();
    }
    match result {
        Err(error) => Err(error),
        Ok(count) => {
            closed?;
            Ok(count)
        }
    }
}
async fn download_on(
    session: &mut Session,
    target: &Location,
    offset: u64,
    expected: &RemoteFile,
    sink: &mut dyn DownloadSink,
) -> Result<()> {
    let before = session.stat(target).await?;
    unchanged(&before, expected)?;
    let timeout = session.options.io_timeout;
    let window = session.options.window;
    let (client, tree) = session.parts();
    let reader = call(timeout, client.open_file_reader(tree, target.key())).await?;
    let result: Result<()> = async {
        unchanged(reader.info(), expected)?;
        let mut reads = FuturesOrdered::new();
        let mut next = offset;
        let mut done = offset;
        while done < expected.size {
            while next < expected.size && reads.len() < window {
                let position = next;
                let length = (expected.size - next).min(CHUNK as u64);
                let reader = &reader;
                reads.push_back(async move {
                    (
                        length,
                        call(timeout, reader.read_at(position, length)).await,
                    )
                });
                next += length;
            }
            let (length, data) = reads.next().await.expect("SMB reads queued");
            let data = data?;
            if data.len() as u64 != length {
                return Err(permanent(
                    "remote SMB file ended early; partial file was retained",
                ));
            }
            sink.write(&data)?;
            done += length;
            tokio::task::yield_now().await;
        }
        Ok(())
    }
    .await;
    if result
        .as_ref()
        .is_err_and(|error| error.is::<TransportError>() || error.is::<InvalidSession>())
    {
        return result;
    }
    let closed = call(timeout, reader.close()).await;
    if closed.is_err() {
        session.client.connection().mark_dead();
    }
    result?;
    closed?;
    unchanged(&session.stat(target).await?, expected)
}
async fn upload_on(
    session: &mut Session,
    local: &Path,
    target: &Location,
    overwrite: bool,
) -> Result<()> {
    let local_error = |error| permanent(format!("local file {local:?}: {error}"));
    let mut file = File::open(local).map_err(local_error)?;
    session
        .select(
            target
                .share
                .as_deref()
                .ok_or_else(|| permanent("select an SMB share before uploading"))?,
        )
        .await?;
    let timeout = session.options.io_timeout;
    let window = session.options.window;
    let (client, tree) = session.parts();
    let writer = if overwrite {
        call(timeout, client.create_file_writer(tree, target.key())).await?
    } else {
        call(
            timeout,
            client.create_file_writer_exclusive(tree, target.key()),
        )
        .await?
    };
    let mut writer = writer
        .with_chunk_size(CHUNK as u32)
        .with_write_behind(WriteBehind::Fixed(window));
    let result: Result<()> = async {
        let mut buffer = [0u8; CHUNK];
        loop {
            let length = file.read(&mut buffer).map_err(local_error)?;
            if length == 0 {
                break;
            }
            call(timeout, writer.write_chunk(&buffer[..length])).await?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            call(timeout, writer.finish()).await?;
            Ok(())
        }
        Err(error) => {
            if !error.is::<TransportError>()
                && !error.is::<InvalidSession>()
                && call(timeout, writer.abort()).await.is_err()
            {
                session.client.connection().mark_dead();
            }
            Err(error)
        }
    }
}
async fn mkdir_on(session: &mut Session, target: &Location, parents: bool) -> Result<()> {
    let Some(share) = &target.share else {
        return if parents {
            Ok(())
        } else {
            Err(permanent("SMB namespace root already exists"))
        };
    };
    session.select(share).await?; // never create shares
    if target.path.is_empty() {
        return if parents {
            Ok(())
        } else {
            Err(permanent("SMB share already exists"))
        };
    }
    let timeout = session.options.io_timeout;
    let (client, tree) = session.parts();
    if !parents {
        return call(timeout, client.create_directory(tree, target.key())).await;
    }
    let mut prefix = String::new();
    for component in target.path.split('/').filter(|part| !part.is_empty()) {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        match raw(timeout, client.stat(tree, &prefix)).await {
            Ok(info) if info.is_directory => continue,
            Ok(_) => return Err(permanent(format!("not an SMB directory: {prefix:?}"))),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(convert(error)),
        }
        match raw(timeout, client.create_directory(tree, &prefix)).await {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if !call(timeout, client.stat(tree, &prefix))
                    .await?
                    .is_directory
                {
                    return Err(permanent(format!("not an SMB directory: {prefix:?}")));
                }
            }
            Err(error) => return Err(convert(error)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> Settings {
        let mut settings = Settings::new();
        Smb.register_settings(&mut settings);
        settings
    }
    #[test]
    fn urls_support_domains_ipv6_and_initial_directories_without_echoing_invalid_input() {
        let target =
            Target::parse("smb://WORKGROUP;alice@server:1445/share/photo%20album", "").unwrap();
        assert_eq!(target.address, "server:1445");
        assert_eq!(target.domain, "WORKGROUP");
        assert_eq!(target.username, "alice");
        assert_eq!(target.home.display(), "/share/photo album/");
        assert_eq!(
            Target::parse("smb://[::1]/share", "DOMAIN")
                .unwrap()
                .address,
            "[::1]:445"
        );
        assert_eq!(
            Target::parse("smb://server", "").unwrap().home.display(),
            "/"
        );
        assert_eq!(
            Target::parse("smb://DOMAIN%5Calice@server/share", "")
                .unwrap()
                .domain,
            "DOMAIN"
        );
        for input in [
            "smb://server/share?password=never-log-this",
            "smb://server/share#never-log-this",
            "smb:///",
            "smb://server:0/share",
            "smb://server/share%2Fpath",
            "smb://server/share/%00",
            "smb://server/share/%FF",
            "sftp://server/share",
        ] {
            let error = Target::parse(input, "").unwrap_err().to_string();
            assert!(!error.contains("never-log-this"), "{error}");
        }
    }
    #[test]
    fn credentials_preserve_empty_overrides_and_resolve_each_source_without_os_defaults() {
        fn bookmark(fields: &str) -> crate::bookmarks::Bookmark {
            toml::from_str(&format!("type = 'smb'\nurl = 'smb://host/share'\n{fields}")).unwrap()
        }
        let mut settings = settings();
        settings.set("smb:user", "default-user").unwrap();
        settings.set("smb:domain", "DEFAULT").unwrap();
        settings.set("smb:password", "global-secret").unwrap();
        for (url, fields, interactive, user, domain, expected_password) in [
            (
                "smb://host/share",
                "",
                None,
                "default-user",
                "DEFAULT",
                Some("global-secret"),
            ),
            (
                "smb://URL;url-user:url%40secret%3Apass@host/share",
                "",
                None,
                "url-user",
                "URL",
                Some("url@secret:pass"),
            ),
            (
                "smb://url-user:@host/share",
                "",
                None,
                "url-user",
                "DEFAULT",
                Some(""),
            ),
            (
                "smb://URL;url-user:url-secret@host/share",
                "user='book-user'\ndomain='BOOK'\npassword='book-secret'",
                None,
                "book-user",
                "BOOK",
                Some("book-secret"),
            ),
            (
                "smb://URL;url-user:url-secret@host/share",
                "user='book-user'\ndomain=''\npassword=''",
                None,
                "book-user",
                "",
                Some(""),
            ),
            (
                "smb://url-user:url-secret@host/share",
                "password='book-secret'",
                Some("interactive-secret"),
                "url-user",
                "DEFAULT",
                Some("interactive-secret"),
            ),
            (
                "smb://URL;url-user:url-secret@host/share",
                "user=''",
                None,
                "",
                "",
                None,
            ),
            ("smb://@host/share", "", None, "", "", None),
            ("smb://:@host/share", "", None, "", "", None),
        ] {
            let book = bookmark(fields);
            let mut target = Target::parse(url, "").unwrap();
            let interactive = interactive.map(|value: &str| Password::new(value.into()));
            let password = target
                .credentials(&settings, Some(&book), interactive.as_ref())
                .unwrap();
            assert_eq!(target.username, user);
            assert_eq!(target.domain, domain);
            assert_eq!(password.as_ref().map(Password::expose), expected_password);
            assert!(target.password.is_none());
        }
        let mut target = Target::parse("smb://host/share", "").unwrap();
        let empty_settings = self::settings();
        assert!(target
            .credentials(&empty_settings, None, None)
            .unwrap()
            .is_none());
        assert!(target.username.is_empty() && target.domain.is_empty());
        let target = Target::parse("smb://user:url-secret@host/share", "").unwrap();
        assert!(!format!("{target:?}").contains("url-secret"));
    }

    #[test]
    fn paths_resolve_in_server_namespace_and_do_not_escape_above_shares_root() {
        let root = Location::default();
        let location = root.resolve("share/parent/photos").unwrap();
        assert_eq!(
            location.resolve("../other").unwrap().display(),
            "/share/parent/other/"
        );
        assert_eq!(
            location.resolve("/second/images").unwrap().display(),
            "/second/images/"
        );
        assert_eq!(location.resolve("../../../../..").unwrap(), root);
        assert_eq!(location.resolve("./").unwrap(), location);
        assert!(root.resolve("/bad\\share/file").is_err());
        assert!(root.resolve("/share/\0bad").is_err());
        assert_eq!(
            root.resolve("/share/file\\name").unwrap().key(),
            "file\\name"
        );
    }
    #[test]
    fn smb_metadata_has_exact_sizes_and_high_resolution_revision_checks_without_fake_unix_ids() {
        let mut info = smb2::FileInfo::default();
        info.size = 123;
        info.created = smb2::pack::FileTime(1);
        info.modified = smb2::pack::FileTime(2);
        info.changed = smb2::pack::FileTime(3);
        let expected = remote_file(&info).unwrap();
        assert_eq!(expected.size, 123);
        assert_eq!(expected.revision.as_deref(), Some("2:3"));
        unchanged(&info, &expected).unwrap();
        let mut opened = info.clone();
        opened.created = opened.modified;
        unchanged(&opened, &expected).unwrap(); // birth time alone may vary across opens
        opened.modified.0 += 1;
        assert!(unchanged(&opened, &expected).is_err());
        info.changed.0 += 1;
        assert!(unchanged(&info, &expected)
            .unwrap_err()
            .is::<NonRetryableError>());
        info.is_directory = true;
        assert!(remote_file(&info).is_err());
        info.is_directory = false;
        info.attributes.0 = FileAttributes::REPARSE_POINT;
        assert!(remote_file(&info).is_err());
        assert!(
            matches!(record("file".into(), 123, smb2::pack::FileTime::ZERO, false), Content::File(file) if file.size == Some(123) && file.unix.is_none() && file.modified.is_none())
        );
    }
    #[test]
    fn auth_and_security_errors_are_permanent_redacted_and_discard_the_session() {
        let error = convert(smb2::Error::Auth {
            message: "password=never-log-this\nprivate".into(),
        });
        assert!(error.is::<NonRetryableError>());
        assert!(error.is::<InvalidSession>());
        assert!(!error.to_string().contains("never-log-this"));
        assert!(!error.to_string().contains('\n'));
        assert!(convert(smb2::Error::Timeout).is::<TransportError>());
        assert!(convert(smb2::Error::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        )))
        .is::<TransportError>());
        let error = convert(smb2::Error::Protocol {
            status: smb2::types::status::NtStatus::ACCESS_DENIED,
            command: smb2::types::Command::Create,
        });
        assert!(error.is::<NonRetryableError>());
        assert!(!error.is::<TransportError>());
    }
    #[test]
    fn settings_reject_unbounded_transfer_windows_and_invalid_timeouts() {
        let mut settings = settings();
        assert_eq!(Options::read(&settings).unwrap().window, 32);
        assert!(!Options::read(&settings).unwrap().dfs);
        settings.values.get_mut("smb:max-credits").unwrap().value = SettingValue::Int(0);
        assert!(Options::read(&settings)
            .unwrap_err()
            .is::<NonRetryableError>());
        settings.values.get_mut("smb:max-credits").unwrap().value = SettingValue::Int(32);
        settings.values.get_mut("smb:dfs").unwrap().value = SettingValue::Bool(true);
        settings
            .values
            .get_mut("smb:require-encryption")
            .unwrap()
            .value = SettingValue::Bool(true);
        assert!(Options::read(&settings).is_err());
    }
    #[tokio::test]
    async fn silent_server_times_out_without_blocking_runtime() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let mut settings = settings();
        settings
            .values
            .get_mut("smb:connect-timeout")
            .unwrap()
            .value = SettingValue::Int(1);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            Smb.connect(ConnectOptions {
                target: format!("smb://{address}/share"),
                bookmark: None,
                settings,
                password: None,
            }),
        )
        .await
        .unwrap();
        assert!(result.err().unwrap().is::<TransportError>());
        server.abort();
    }
    #[test]
    fn invalid_smb_urls_fail_before_creating_a_credential_bearing_job() {
        let mut app = crate::core::Daemon::bootstrap().unwrap();
        let result = app.command("connect smb://user:never-log-this@host/share?invalid=secret");
        assert!(!result.join("\n").contains("never-log-this"));
        assert!(app.jobs.jobs.is_empty());
    }
}
