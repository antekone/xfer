/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Async HTTP/WebDAV transport. Listings are namespace-aware streaming XML;
//! file bodies are bounded streams, never whole-file allocations.
use crate::{
    backend::{Backend, ConnectOptions, Connection},
    bookmarks::Bookmark,
    core::{SettingValue, Settings},
    credentials::Password,
    error::NonRetryableError,
    listing::{Content, DirectoryInfo, FileInfo, ListingSink},
    transfer::{DownloadSink, RemoteFile},
};
use anyhow::Result;
use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, Full, StreamBody};
// These crates keep the plain names under their version-1 stack; see Cargo.toml.
use hyper::{
    body::{Bytes, Frame, Incoming},
    header::{self, HeaderMap, HeaderValue},
    Method, Request, Response, StatusCode,
};
use hyper_1 as hyper;
use hyper_rustls_1 as hyper_rustls;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use percent_encoding::percent_decode_str;
use quick_xml::{
    events::Event,
    name::{Namespace, ResolveResult},
    NsReader,
};
use rustls_1 as rustls;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};
use url::Url;

pub struct Webdav;
const CHUNK: usize = 64 * 1024;
const XML_TOKEN: usize = 1024 * 1024;
const FIELD: usize = 64 * 1024;
const PROPERTIES: &str = "<d:propfind xmlns:d=\"DAV:\"><d:prop><d:resourcetype/><d:getcontentlength/><d:getlastmodified/><d:getetag/></d:prop></d:propfind>";
fn permanent(message: impl Into<String>) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}
fn invalid_xml() -> anyhow::Error {
    permanent("invalid or unsupported WebDAV XML response")
}

/// Logical, decoded path segments. User commands are NOT URL-decoded again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Location(Vec<String>);
impl Location {
    fn resolve(&self, input: &str) -> Result<Self> {
        if input.len() > FIELD || input.chars().any(char::is_control) || input.contains('\\') {
            return Err(permanent("invalid WebDAV path"));
        }
        let mut parts = if input.starts_with('/') {
            Vec::new()
        } else {
            self.0.clone()
        };
        for part in input.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop();
                }
                _ => parts.push(part.to_owned()),
            }
        }
        Ok(Self(parts))
    }
    fn from_url(url: &Url) -> Result<Self> {
        let mut parts = Vec::new();
        for part in url.path().split('/').filter(|part| !part.is_empty()) {
            let part = percent_decode_str(part)
                .decode_utf8()
                .map_err(|_| permanent("WebDAV paths must be UTF-8"))?;
            if matches!(part.as_ref(), "." | "..")
                || part.contains(['/', '\\'])
                || part.chars().any(char::is_control)
            {
                return Err(permanent("invalid encoded WebDAV path"));
            }
            parts.push(part.into_owned());
        }
        if url.path().len() > FIELD {
            return Err(permanent("WebDAV path is too long"));
        }
        Ok(Self(parts))
    }
    fn display(&self, directory: bool) -> String {
        if self.0.is_empty() {
            "/".into()
        } else {
            format!("/{}{}", self.0.join("/"), if directory { "/" } else { "" })
        }
    }
    fn url(&self, base: &Url, directory: bool) -> Url {
        let mut url = base.clone();
        {
            let mut segments = url.path_segments_mut().expect("validated HTTP origin");
            segments.clear();
            for part in &self.0 {
                segments.push(part);
            }
            if directory && !self.0.is_empty() {
                segments.push("");
            }
        }
        url
    }
}
fn directory_input(path: &str) -> bool {
    path.is_empty() || path.ends_with('/') || matches!(path.rsplit('/').next(), Some("." | ".."))
}
fn seconds(settings: &Settings, key: &str) -> Result<Duration> {
    match settings.values.get(key).map(|setting| &setting.value) {
        Some(SettingValue::Int(value @ 1..=300)) => Ok(Duration::from_secs(*value as u64)),
        _ => Err(permanent(format!("{key} must be an integer from 1 to 300"))),
    }
}
struct Target {
    user: String,
    base: Url,
    home: Location,
    auth: Option<HeaderValue>,
}
impl Target {
    fn read(
        value: &str,
        settings: &Settings,
        bookmark: Option<&Bookmark>,
        password: Option<&Password>,
    ) -> Result<Self> {
        if value.len() > FIELD * 2 {
            return Err(permanent("WebDAV URL is too long"));
        }
        let (scheme, rest) = value
            .split_once("://")
            .ok_or_else(|| permanent("expected an HTTP(S) or DAV(S) URL"))?;
        let scheme = match scheme {
            "https" | "davs" | "webdavs" => "https",
            "http" | "dav" | "webdav" => "http",
            _ => return Err(permanent("expected an HTTP(S) or DAV(S) URL")),
        };
        let mut url = Url::parse(&format!("{scheme}://{rest}"))
            .map_err(|_| permanent("invalid WebDAV URL"))?;
        if url.host_str().is_none()
            || url.port() == Some(0)
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(permanent(
                "WebDAV URL requires a host and no query or fragment",
            ));
        }
        let home = Location::from_url(&url)?;
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let info = authority.rsplit_once('@').map(|(info, _)| info);
        let decode = |value: &str| {
            percent_decode_str(value)
                .decode_utf8()
                .map(|value| value.into_owned())
                .map_err(|_| permanent("WebDAV URL credentials must be UTF-8"))
        };
        let user = match bookmark.and_then(|bookmark| bookmark.user.as_deref()) {
            Some(user) => user.to_owned(),
            None if info.is_some() => decode(url.username())?,
            None => settings.text("webdav:user").unwrap_or("").to_owned(),
        };
        let url_password = info
            .and_then(|info| info.split_once(':'))
            .map(|(_, value)| decode(value).map(Password::new))
            .transpose()?;
        let password = password
            .cloned()
            .or_else(|| bookmark.and_then(|bookmark| bookmark.password.clone()))
            .or(url_password)
            .or_else(|| settings.secret("webdav:password").cloned());
        if user.contains(':') || user.chars().any(char::is_control) || user.len() > FIELD {
            return Err(permanent("invalid WebDAV Basic-auth username"));
        }
        let auth = if user.is_empty() {
            None
        } else {
            if scheme == "http" && !settings.bool("webdav:allow-insecure-auth") {
                return Err(permanent("WebDAV credentials require HTTPS; webdav:allow-insecure-auth explicitly permits plaintext HTTP"));
            }
            let raw = zeroize::Zeroizing::new(format!(
                "{user}:{}",
                password.as_ref().map(Password::expose).unwrap_or("")
            ));
            if raw.len() > FIELD {
                return Err(permanent("WebDAV credentials are too long"));
            }
            let encoded = zeroize::Zeroizing::new(
                base64::engine::general_purpose::STANDARD.encode(raw.as_bytes()),
            );
            let mut header = HeaderValue::from_str(&format!("Basic {}", encoded.as_str()))
                .map_err(|_| permanent("invalid WebDAV credentials"))?;
            header.set_sensitive(true);
            Some(header)
        };
        let _ = url.set_password(None);
        let _ = url.set_username("");
        url.set_path("/");
        Ok(Self {
            user,
            base: url,
            home,
            auth,
        })
    }
}
pub(crate) fn validate_target(
    value: &str,
    settings: &Settings,
    bookmark: Option<&Bookmark>,
) -> Result<()> {
    Target::read(value, settings, bookmark, None)?;
    seconds(settings, "webdav:connect-timeout")?;
    seconds(settings, "webdav:operation-timeout")?;
    Ok(())
}

#[derive(Debug)]
struct SkipCertificateValidation;
impl rustls::client::danger::ServerCertVerifier for SkipCertificateValidation {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .mapping
            .iter()
            .map(|(scheme, _)| *scheme)
            .collect()
    }
}
type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>;
/// Request/parse body type: bounded chunks with a boxed error so response
/// bodies (hyper::Error), file streams (io::Error), and static data unify.
type Body = UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;
fn full_body(data: impl Into<Bytes>) -> Body {
    Full::new(data.into())
        .map_err(|err| match err {})
        .boxed_unsync()
}
fn empty_body() -> Body {
    Empty::<Bytes>::new()
        .map_err(|err| match err {})
        .boxed_unsync()
}
struct WebdavConnection {
    user: String,
    client: HttpClient,
    base: Url,
    cwd: Location,
    home: Location,
    auth: Option<HeaderValue>,
    timeout: Duration,
}
#[derive(Debug)]
struct HttpFailure(u16);
impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebDAV HTTP {}", self.0)
    }
}
impl std::error::Error for HttpFailure {}
fn http_error(status: u16) -> anyhow::Error {
    let error = anyhow::Error::new(HttpFailure(status));
    if matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504) {
        error
    } else {
        error.context(NonRetryableError(format!("WebDAV HTTP {status}")))
    }
}
fn has_status(error: &anyhow::Error, codes: &[u16]) -> bool {
    error
        .downcast_ref::<HttpFailure>()
        .is_some_and(|error| codes.contains(&error.0))
}
fn transport_error(error: hyper_util::client::legacy::Error) -> anyhow::Error {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(cause) = source {
        if cause.is::<rustls::Error>()
            || cause.downcast_ref::<io::Error>().is_some_and(|error| {
                error.kind() == io::ErrorKind::InvalidData
                    || error
                        .get_ref()
                        .is_some_and(|inner| inner.is::<rustls::Error>())
            })
        {
            return permanent("WebDAV TLS handshake or certificate verification failed");
        }
        // io::Error::source skips its immediate boxed error; walk get_ref
        // instead so nested connector IO wrappers cannot hide TLS failures.
        source = match cause.downcast_ref::<io::Error>() {
            Some(error) => error
                .get_ref()
                .map(|inner| inner as &(dyn std::error::Error + 'static)),
            None => cause.source(),
        };
    }
    anyhow::anyhow!("WebDAV transport failed")
}
fn body_error(error: hyper::Error) -> anyhow::Error {
    if error.is_parse() || error.is_user() {
        permanent("invalid WebDAV HTTP response or request body")
    } else {
        anyhow::anyhow!("WebDAV transport failed")
    }
}
impl WebdavConnection {
    async fn request(
        &self,
        method: Method,
        mut url: Url,
        headers: HeaderMap,
        body: Body,
    ) -> Result<Response<Incoming>> {
        let mut body = Some(body);
        for attempt in 0..=3 {
            let mut request = Request::builder().method(method.clone()).uri(url.as_str());
            *request.headers_mut().unwrap() = headers.clone();
            if let Some(auth) = &self.auth {
                request = request.header(header::AUTHORIZATION, auth.clone());
            }
            let request = request
                .body(body.take().unwrap_or_else(empty_body))
                .map_err(|_| permanent("invalid WebDAV request"))?;
            let mut response = tokio::time::timeout(self.timeout, self.client.request(request))
                .await
                .map_err(|_| anyhow::anyhow!("WebDAV operation timed out"))?
                .map_err(transport_error)?;
            if !response.status().is_redirection() {
                response.extensions_mut().insert(url);
                return Ok(response);
            }
            if attempt == 3 || !matches!(method.as_str(), "PROPFIND" | "GET" | "HEAD") {
                return Err(permanent("WebDAV redirect refused"));
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .filter(|value| value.len() <= FIELD)
                .ok_or_else(|| permanent("invalid WebDAV redirect"))?;
            let next = url
                .join(location)
                .map_err(|_| permanent("invalid WebDAV redirect"))?;
            if next.origin() != self.base.origin()
                || !next.username().is_empty()
                || next.password().is_some()
                || next.query().is_some()
                || next.fragment().is_some()
                || Location::from_url(&next)? != Location::from_url(&url)?
                || next == url
            {
                return Err(permanent(
                    "WebDAV redirects may only canonicalize a same-origin trailing slash",
                ));
            }
            // PROPFIND's fixed body is replayable; never replay file uploads.
            if method.as_str() == "PROPFIND" {
                body = Some(full_body(Bytes::from_static(PROPERTIES.as_bytes())));
            }
            url = next;
        }
        unreachable!()
    }
    async fn propfind(
        &self,
        path: &Location,
        directory: bool,
        depth: u8,
        emit: &mut (dyn FnMut(Entry) -> Result<()> + Send),
    ) -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "depth",
            HeaderValue::from_static(if depth == 0 { "0" } else { "1" }),
        );
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml; charset=utf-8"),
        );
        let url = path.url(&self.base, directory);
        let response = self
            .request(
                Method::from_bytes(b"PROPFIND").unwrap(),
                url.clone(),
                headers,
                full_body(Bytes::from_static(PROPERTIES.as_bytes())),
            )
            .await?;
        if response.status() != StatusCode::MULTI_STATUS {
            if response.status().is_success() {
                return Err(permanent(format!(
                    "expected WebDAV 207 Multi-Status, got HTTP {} (HTML indexes are not WebDAV)",
                    response.status().as_u16()
                )));
            }
            return Err(http_error(response.status().as_u16()));
        }
        let url = response.extensions().get::<Url>().cloned().unwrap_or(url);
        parse_xml(
            response
                .into_body()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                .boxed_unsync(),
            self.timeout,
            &mut |raw| {
                let href = raw.href.ok_or_else(invalid_xml)?;
                let href = url.join(href.trim()).map_err(|_| invalid_xml())?;
                if href.origin() != self.base.origin()
                    || !href.username().is_empty()
                    || href.password().is_some()
                    || href.query().is_some()
                    || href.fragment().is_some()
                {
                    return Err(permanent("WebDAV returned an unsafe resource href"));
                }
                let location = Location::from_url(&href)?;
                if location != *path
                    && (depth == 0
                        || location.0.len() != path.0.len() + 1
                        || !location.0.starts_with(&path.0))
                {
                    return Err(permanent(
                        "WebDAV returned a resource outside the requested depth",
                    ));
                }
                let directory = raw
                    .props
                    .directory
                    .ok_or_else(|| permanent("WebDAV omitted DAV:resourcetype"))?;
                let size = raw
                    .props
                    .size
                    .map(|value| value.trim().parse::<u64>().map_err(|_| invalid_xml()))
                    .transpose()?;
                let modified = raw
                    .props
                    .modified
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| parse_date(&value))
                    .transpose()?;
                let etag = raw
                    .props
                    .etag
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| property_etag(value.trim()))
                    .transpose()?;
                emit(Entry {
                    location,
                    directory,
                    size,
                    modified,
                    etag,
                })
            },
        )
        .await
    }
    async fn stat(&self, location: &Location, directory: bool) -> Result<Entry> {
        let mut entry = None;
        self.propfind(location, directory, 0, &mut |next| {
            if entry.is_some() {
                return Err(permanent("duplicate WebDAV stat response"));
            }
            entry = Some(next);
            Ok(())
        })
        .await?;
        entry.ok_or_else(|| permanent("WebDAV stat returned no resource"))
    }
    async fn mkcol(&self, location: &Location) -> Result<()> {
        let response = self
            .request(
                Method::from_bytes(b"MKCOL").unwrap(),
                location.url(&self.base, true),
                HeaderMap::new(),
                empty_body(),
            )
            .await?;
        if matches!(response.status().as_u16(), 200 | 201 | 204) {
            Ok(())
        } else {
            Err(http_error(response.status().as_u16()))
        }
    }
}
fn parse_date(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value.trim())
        .map(|date| date.with_timezone(&Utc))
        .map_err(|_| permanent("invalid WebDAV modification date"))
}
// Some deployed servers (including WsgiDAV) omit quotes in the DAV property,
// while correctly quoting the HTTP ETag header. Normalize only that property;
// conditionals and GET headers still compare exact, quoted entity tags.
fn property_etag(value: &str) -> Result<String> {
    if value.starts_with('"') || value.starts_with("W/") {
        return valid_etag(value).map(str::to_owned);
    }
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte))
    {
        Ok(format!("\"{value}\""))
    } else {
        Err(permanent("invalid WebDAV ETag"))
    }
}
fn valid_etag(value: &str) -> Result<&str> {
    let quoted = value.strip_prefix("W/").unwrap_or(value);
    if quoted.len() < 2
        || !quoted.starts_with('"')
        || !quoted.ends_with('"')
        || quoted[1..quoted.len() - 1].contains('"')
        || value.chars().any(char::is_control)
    {
        return Err(permanent("invalid WebDAV ETag"));
    }
    Ok(value)
}
fn header_text(headers: &HeaderMap, name: header::HeaderName) -> Result<Option<&str>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| permanent("invalid WebDAV response header"))
        })
        .transpose()
}
#[derive(Default)]
struct Entry {
    location: Location,
    directory: bool,
    size: Option<u64>,
    modified: Option<DateTime<Utc>>,
    etag: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Revision {
    etag: Option<String>,
    modified: Option<DateTime<Utc>>,
}
impl Entry {
    fn remote(&self) -> Result<RemoteFile> {
        if self.directory {
            return Err(permanent("expected a WebDAV file, not a collection"));
        }
        Ok(RemoteFile {
            size: self
                .size
                .ok_or_else(|| permanent("WebDAV omitted file size"))?,
            revision: Some(
                serde_json::to_string(&Revision {
                    etag: self.etag.clone(),
                    modified: self.modified,
                })
                .unwrap(),
            ),
        })
    }
    fn content(self) -> Content {
        let name = self
            .location
            .0
            .last()
            .cloned()
            .unwrap_or_else(|| "/".into());
        if self.directory {
            Content::Directory(DirectoryInfo {
                name,
                size: self.size,
                modified: self.modified,
                unix: None,
            })
        } else {
            Content::File(FileInfo {
                name,
                size: self.size,
                modified: self.modified,
                unix: None,
            })
        }
    }
}
fn unchanged(entry: &Entry, expected: &RemoteFile) -> Result<()> {
    let actual = entry.remote()?;
    if actual.size != expected.size || actual.revision != expected.revision {
        Err(permanent(
            "remote WebDAV file changed; partial file was retained",
        ))
    } else {
        Ok(())
    }
}

#[async_trait]
impl Backend for Webdav {
    fn protocol(&self) -> &'static str {
        "webdav"
    }
    fn register_settings(&self, settings: &mut Settings) {
        settings.register(
            "webdav:user",
            SettingValue::Text(String::new()),
            "Default WebDAV Basic-auth user; empty means anonymous",
        );
        settings.register(
            "webdav:password",
            SettingValue::Secret(None),
            "Default WebDAV password (redacted)",
        );
        settings.register(
            "webdav:allow-insecure-auth",
            SettingValue::Bool(false),
            "Allow Basic credentials over plaintext HTTP (unsafe; reconnect to apply)",
        );
        settings.register(
            "webdav:connect-timeout",
            SettingValue::Int(10),
            "WebDAV TCP connect timeout, 1..300 seconds",
        );
        settings.register(
            "webdav:operation-timeout",
            SettingValue::Int(30),
            "WebDAV request/read timeout, 1..300 seconds",
        );
    }
    async fn connect(&self, options: ConnectOptions) -> Result<Box<dyn Connection>> {
        let target = Target::read(
            options
                .bookmark
                .as_ref()
                .map(|bookmark| bookmark.url.as_str())
                .unwrap_or(&options.target),
            &options.settings,
            options.bookmark.as_ref(),
            options.password.as_ref(),
        )?;
        let timeout = seconds(&options.settings, "webdav:operation-timeout")?;
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(Some(seconds(&options.settings, "webdav:connect-timeout")?));
        let builder = hyper_rustls::HttpsConnectorBuilder::new();
        let builder = if options.settings.bool("ssl:skip-validation") {
            builder.with_tls_config(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(SkipCertificateValidation))
                    .with_no_client_auth(),
            )
        } else {
            builder.with_native_roots()?
        };
        let client = Client::builder(TokioExecutor::new())
            .http1_max_buf_size(CHUNK)
            .build(builder.https_or_http().enable_http1().wrap_connector(tcp));
        let connection = WebdavConnection {
            user: target.user,
            client,
            base: target.base,
            cwd: target.home.clone(),
            home: target.home,
            auth: target.auth,
            timeout,
        };
        if !connection.stat(&connection.cwd, true).await?.directory {
            return Err(permanent("WebDAV connection URL must select a collection"));
        }
        Ok(Box::new(connection))
    }
}
#[async_trait]
impl Connection for WebdavConnection {
    fn prompt_user(&self) -> String {
        self.user.clone()
    }
    fn prompt_host(&self) -> String {
        self.base.host_str().unwrap_or("").to_owned()
    }

    fn protocol(&self) -> &'static str {
        "webdav"
    }
    fn pwd(&self) -> Result<String> {
        Ok(self.cwd.display(true))
    }
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        Ok(match path {
            Some(path) => self.cwd.resolve(path)?,
            None => self.home.clone(),
        }
        .display(true))
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        Ok(self.cwd.resolve(path)?.display(directory_input(path)))
    }
    async fn change_directory(&mut self, path: &str) -> Result<()> {
        let location = self.cwd.resolve(path)?;
        if !self.stat(&location, true).await?.directory {
            return Err(permanent("not a WebDAV collection"));
        }
        self.cwd = location;
        Ok(())
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let location = self.cwd.resolve(path)?;
        let entry = self.stat(&location, directory_input(path)).await?;
        if !entry.directory {
            if directory_input(path) {
                return Err(permanent("not a WebDAV collection"));
            }
            output.emit_file(entry.content())?;
            return Ok(1);
        }
        let mut count = 0;
        let mut saw_self = false;
        self.propfind(&location, true, 1, &mut |entry| {
            if entry.location == location {
                if saw_self || !entry.directory {
                    return Err(invalid_xml());
                }
                saw_self = true;
            } else {
                output.emit(entry.content())?;
                count += 1;
            }
            Ok(())
        })
        .await?;
        if !saw_self {
            return Err(permanent("WebDAV listing omitted its collection"));
        }
        Ok(count)
    }
    async fn stat_file(&self, path: &str) -> Result<RemoteFile> {
        self.stat(&self.cwd.resolve(path)?, directory_input(path))
            .await?
            .remote()
    }
    async fn download(
        &self,
        path: &str,
        offset: u64,
        expected: &RemoteFile,
        sink: &mut dyn DownloadSink,
    ) -> Result<()> {
        if offset > expected.size {
            return Err(permanent("resume offset exceeds WebDAV file size"));
        }
        let location = self.cwd.resolve(path)?;
        unchanged(&self.stat(&location, false).await?, expected)?;
        if offset == expected.size {
            return Ok(());
        }
        let revision: Revision = serde_json::from_str(expected.revision.as_deref().unwrap_or(""))
            .map_err(|_| permanent("invalid WebDAV revision"))?;
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
        if let Some(etag) = revision
            .etag
            .as_deref()
            .filter(|etag| !etag.starts_with("W/"))
        {
            headers.insert(
                header::IF_MATCH,
                HeaderValue::from_str(etag).map_err(|_| permanent("invalid WebDAV revision"))?,
            );
        } else if let Some(modified) = revision.modified {
            headers.insert(
                header::IF_UNMODIFIED_SINCE,
                HeaderValue::from_str(&modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
                    .unwrap(),
            );
        } else if offset != 0 {
            return Err(permanent(
                "WebDAV server supplies no strong ETag or modification date; cannot resume safely",
            ));
        }
        if offset != 0 {
            headers.insert(
                header::RANGE,
                HeaderValue::from_str(&format!("bytes={offset}-")).unwrap(),
            );
        }
        let mut response = self
            .request(
                Method::GET,
                location.url(&self.base, false),
                headers,
                empty_body(),
            )
            .await?;
        let status = response.status().as_u16();
        if !matches!(status, 200 | 206) {
            return Err(http_error(status));
        }
        if status != if offset == 0 { 200 } else { 206 } {
            return Err(permanent(
                "WebDAV server did not honor the requested byte range",
            ));
        }
        if let Some(encoding) = header_text(response.headers(), header::CONTENT_ENCODING)? {
            if !encoding.eq_ignore_ascii_case("identity") {
                return Err(permanent("unexpected compressed WebDAV download"));
            }
        }
        if let Some(etag) = &revision.etag {
            if header_text(response.headers(), header::ETAG)? != Some(etag.as_str()) {
                return Err(permanent("remote WebDAV ETag changed"));
            }
        }
        if let Some(modified) = revision.modified {
            if header_text(response.headers(), header::LAST_MODIFIED)?
                .map(parse_date)
                .transpose()?
                != Some(modified)
            {
                return Err(permanent("remote WebDAV modification date changed"));
            }
        }
        let remaining = expected.size - offset;
        if let Some(length) = header_text(response.headers(), header::CONTENT_LENGTH)? {
            if length.parse::<u64>().ok() != Some(remaining) {
                return Err(permanent("unexpected WebDAV download length"));
            }
        }
        if offset != 0 {
            let expected_range = format!("bytes {offset}-{}/{}", expected.size - 1, expected.size);
            if header_text(response.headers(), header::CONTENT_RANGE)?
                != Some(expected_range.as_str())
            {
                return Err(permanent("invalid WebDAV Content-Range"));
            }
        }
        let mut received = 0u64;
        loop {
            let Some(frame) = tokio::time::timeout(self.timeout, response.body_mut().frame())
                .await
                .map_err(|_| anyhow::anyhow!("WebDAV read timed out"))?
                .transpose()
                .map_err(body_error)?
            else {
                break;
            };
            let Some(chunk) = frame.data_ref() else {
                continue;
            };
            if chunk.len() as u64 > remaining - received {
                return Err(permanent("WebDAV sent more bytes than expected"));
            }
            for bytes in chunk.chunks(CHUNK) {
                sink.write(bytes)?;
                received += bytes.len() as u64;
                tokio::task::yield_now().await;
            }
        }
        if received != remaining {
            return Err(anyhow::anyhow!(
                "WebDAV download ended before the expected size"
            ));
        }
        unchanged(&self.stat(&location, false).await?, expected)
    }
    async fn remove_file(&self, path: &str) -> Result<()> {
        crate::backend::validate_file_path(path)?;
        let location = self.cwd.resolve(path)?;
        let entry = self.stat(&location, false).await?;
        if entry.directory {
            return Err(permanent("rm refuses WebDAV collections"));
        }
        let tag = entry
            .etag
            .filter(|tag| !tag.starts_with("W/"))
            .ok_or_else(|| permanent("safe WebDAV rm requires a strong ETag"))?;
        // A DELETE against a collection is recursive. Check non-destructively
        // that the server enforces If-Match before relying on its condition.
        let url = location.url(&self.base, false);
        let mut probe = HeaderMap::new();
        probe.insert(
            header::IF_MATCH,
            HeaderValue::from_static("\"xfer-impossible-validator\""),
        );
        if tag == "\"xfer-impossible-validator\"" {
            return Err(permanent("cannot safely validate WebDAV delete conditions"));
        }
        let response = self
            .request(Method::HEAD, url.clone(), probe, empty_body())
            .await?;
        if response.status() != StatusCode::PRECONDITION_FAILED {
            return Err(permanent(
                "safe WebDAV rm requires enforced If-Match conditions",
            ));
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_MATCH,
            HeaderValue::from_str(&tag).map_err(|_| invalid_xml())?,
        );
        let response = self
            .request(Method::DELETE, url, headers, empty_body())
            .await?;
        if matches!(response.status().as_u16(), 200 | 204) {
            Ok(())
        } else {
            Err(http_error(response.status().as_u16()))
        }
    }
    async fn rename_file(&self, source: &str, destination: &str, overwrite: bool) -> Result<()> {
        crate::backend::validate_rename_paths(source, destination)?;
        let source = self.cwd.resolve(source)?;
        let destination = self.cwd.resolve(destination)?;
        let entry = self.stat(&source, false).await?;
        if entry.directory {
            return Err(permanent("mv expects a WebDAV file, not a collection"));
        }
        if source == destination {
            return Ok(());
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "destination",
            HeaderValue::from_str(destination.url(&self.base, false).as_str())
                .map_err(|_| permanent("invalid WebDAV move destination"))?,
        );
        let mut replace = false;
        if overwrite {
            match self.stat(&destination, false).await {
                Ok(target) => {
                    if target.directory {
                        return Err(permanent("mv cannot replace a WebDAV collection"));
                    }
                    let tag = target
                        .etag
                        .filter(|tag| !tag.starts_with("W/"))
                        .ok_or_else(|| {
                            permanent("replacing a WebDAV file requires a strong destination ETag")
                        })?;
                    let url = destination.url(&self.base, false);
                    // Verify support non-destructively. Some deployed servers
                    // silently ignore absolute resource tags; MOVE Overwrite:T
                    // could then delete a substituted collection recursively.
                    let mut probe = HeaderMap::new();
                    probe.insert(
                        "if",
                        HeaderValue::from_str(&format!("<{url}> (Not [{tag}])"))
                            .map_err(|_| invalid_xml())?,
                    );
                    let response = self
                        .request(Method::HEAD, url.clone(), probe, empty_body())
                        .await?;
                    if response.status() != StatusCode::PRECONDITION_FAILED {
                        return Err(permanent("safe WebDAV replacement requires enforced tagged If conditions; destination may have changed or server lacks support"));
                    }
                    let mut condition = format!("<{url}> ([{tag}])");
                    if let Some(source_tag) =
                        entry.etag.as_ref().filter(|tag| !tag.starts_with("W/"))
                    {
                        condition.push_str(&format!(
                            " <{}> ([{source_tag}])",
                            source.url(&self.base, false)
                        ));
                    }
                    headers.insert(
                        "if",
                        HeaderValue::from_str(&condition).map_err(|_| invalid_xml())?,
                    );
                    replace = true;
                }
                Err(error) if has_status(&error, &[404]) => {}
                Err(error) => return Err(error),
            }
        }
        headers.insert(
            "overwrite",
            HeaderValue::from_static(if replace { "T" } else { "F" }),
        );
        // For replacing moves use resource-tagged conditions above: some
        // servers wrongly apply HTTP If-Match to both source and destination.
        if !replace {
            if let Some(etag) = entry.etag.filter(|tag| !tag.starts_with("W/")) {
                headers.insert(
                    header::IF_MATCH,
                    HeaderValue::from_str(&etag).map_err(|_| invalid_xml())?,
                );
            } else if let Some(modified) = entry.modified {
                headers.insert(
                    header::IF_UNMODIFIED_SINCE,
                    HeaderValue::from_str(
                        &modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
                    )
                    .unwrap(),
                );
            }
        }
        let response = self
            .request(
                Method::from_bytes(b"MOVE").unwrap(),
                source.url(&self.base, false),
                headers,
                empty_body(),
            )
            .await?;
        if matches!(response.status().as_u16(), 201 | 204) {
            Ok(())
        } else {
            Err(http_error(response.status().as_u16()))
        }
    }
    async fn upload(&self, local: &Path, path: &str, overwrite: bool) -> Result<()> {
        self.upload_with_progress(
            local,
            path,
            overwrite,
            crate::transfer::UploadProgress::silent(),
        )
        .await
    }
    async fn upload_with_progress(
        &self,
        local: &Path,
        path: &str,
        overwrite: bool,
        progress: crate::transfer::UploadProgress,
    ) -> Result<()> {
        let location = self.cwd.resolve(path)?;
        if location.0.is_empty() || directory_input(path) {
            return Err(permanent("put expects a WebDAV filename"));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(local)
            .map_err(|_| permanent("cannot open local upload file"))?;
        let before = file
            .metadata()
            .map_err(|_| permanent("cannot stat local upload file"))?;
        if !before.is_file() {
            return Err(permanent("upload requires a regular local file"));
        }
        let check = file
            .try_clone()
            .map_err(|_| permanent("cannot retain local upload handle"))?;
        let sent = Arc::new(AtomicU64::new(0));
        let counter = sent.clone();
        let stream = futures_util::stream::try_unfold(
            (file, before.len(), counter, progress),
            |(mut file, remaining, sent, progress)| async move {
                if remaining == 0 {
                    return Ok::<_, io::Error>(None);
                }
                tokio::task::yield_now().await;
                let mut data = vec![0; remaining.min(CHUNK as u64) as usize];
                let count = file.read(&mut data)?;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "local upload file changed",
                    ));
                }
                data.truncate(count);
                sent.fetch_add(count as u64, Ordering::Relaxed);
                progress.advance(count as u64);
                Ok(Some((
                    Bytes::from(data),
                    (file, remaining - count as u64, sent, progress),
                )))
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&before.len().to_string()).unwrap(),
        );
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        if !overwrite {
            headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        }
        let response = self
            .request(
                Method::PUT,
                location.url(&self.base, false),
                headers,
                BodyExt::map_err(
                    StreamBody::new(stream.map_ok(Frame::data)),
                    |error: io::Error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>,
                )
                .boxed_unsync(),
            )
            .await?;
        if !matches!(response.status().as_u16(), 200 | 201 | 204) {
            return Err(http_error(response.status().as_u16()));
        }
        let after = check
            .metadata()
            .map_err(|_| permanent("cannot stat local upload file"))?;
        if sent.load(Ordering::Relaxed) != before.len()
            || before.len() != after.len()
            || before.modified().ok() != after.modified().ok()
        {
            return Err(permanent(
                "local file changed or upload was acknowledged before completion",
            ));
        }
        Ok(())
    }
    async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        let location = self.cwd.resolve(path)?;
        if !parents {
            if location.0.is_empty() {
                return Err(permanent("WebDAV root already exists"));
            }
            return self.mkcol(&location).await;
        }
        if location.0.is_empty() {
            if self.stat(&location, true).await?.directory {
                return Ok(());
            }
            return Err(permanent("not a WebDAV collection"));
        }
        for count in 1..=location.0.len() {
            let ancestor = Location(location.0[..count].to_vec());
            match self.stat(&ancestor, false).await {
                Ok(entry) if entry.directory => continue,
                Ok(_) => return Err(permanent("WebDAV parent is a file")),
                Err(error) if has_status(&error, &[404]) => {}
                Err(error) => return Err(error),
            }
            if let Err(error) = self.mkcol(&ancestor).await {
                if !has_status(&error, &[405, 409, 412])
                    || !self.stat(&ancestor, false).await?.directory
                {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

// Per-token input budget prevents a huge unterminated XML tag/text from growing
// quick-xml's event buffer without bound. No cap on the number of resources.
struct XmlInput {
    body: Body,
    chunk: Bytes,
    used: usize,
}
impl AsyncBufRead for XmlInput {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.used >= XML_TOKEN {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebDAV XML token limit",
            )));
        }
        while this.chunk.is_empty() {
            match hyper::body::Body::poll_frame(Pin::new(&mut this.body), cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(frame))) => this.chunk = frame.into_data().unwrap_or_default(),
                Poll::Ready(Some(Err(error))) => {
                    let parse = error
                        .downcast_ref::<hyper::Error>()
                        .map(|e| e.is_parse())
                        .unwrap_or(false);
                    return Poll::Ready(Err(io::Error::new(
                        if parse {
                            io::ErrorKind::InvalidData
                        } else {
                            io::ErrorKind::ConnectionReset
                        },
                        "WebDAV response stream failed",
                    )));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(&[])),
            }
        }
        Poll::Ready(Ok(
            &this.chunk[..this.chunk.len().min(XML_TOKEN - this.used)]
        ))
    }
    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.chunk = this.chunk.slice(amount..);
        this.used += amount;
    }
}
impl AsyncRead for XmlInput {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let bytes = match self.as_mut().poll_fill_buf(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(bytes)) => bytes,
        };
        let count = bytes.len().min(buf.remaining());
        buf.put_slice(&bytes[..count]);
        self.consume(count);
        Poll::Ready(Ok(()))
    }
}
#[derive(Default)]
struct Props {
    directory: Option<bool>,
    size: Option<String>,
    modified: Option<String>,
    etag: Option<String>,
}
impl Props {
    fn merge(&mut self, other: Self) {
        if other.directory.is_some() {
            self.directory = other.directory;
        }
        if other.size.is_some() {
            self.size = other.size;
        }
        if other.modified.is_some() {
            self.modified = other.modified;
        }
        if other.etag.is_some() {
            self.etag = other.etag;
        }
    }
}
#[derive(Default)]
struct RawEntry {
    href: Option<String>,
    props: Props,
}
fn path_is(stack: &[Vec<u8>], path: &[&[u8]]) -> bool {
    stack.len() == path.len() && stack.iter().zip(path).all(|(left, right)| left == right)
}
fn leaf(stack: &[Vec<u8>]) -> bool {
    path_is(stack, &[b"multistatus", b"response", b"href"])
        || path_is(stack, &[b"multistatus", b"response", b"status"])
        || path_is(
            stack,
            &[b"multistatus", b"response", b"propstat", b"status"],
        )
        || (stack.len() == 5
            && path_is(
                &stack[..4],
                &[b"multistatus", b"response", b"propstat", b"prop"],
            )
            && matches!(
                stack[4].as_slice(),
                b"getcontentlength" | b"getlastmodified" | b"getetag"
            ))
}
fn status(text: &str) -> Result<u16> {
    let mut words = text.split_whitespace();
    if !words.next().is_some_and(|value| value.starts_with("HTTP/")) {
        return Err(invalid_xml());
    }
    words
        .next()
        .and_then(|value| value.parse().ok())
        .filter(|value| (100..600).contains(value))
        .ok_or_else(invalid_xml)
}
async fn parse_xml(
    body: Body,
    timeout: Duration,
    emit: &mut (dyn FnMut(RawEntry) -> Result<()> + Send),
) -> Result<()> {
    let mut reader = NsReader::from_reader(XmlInput {
        body,
        chunk: Bytes::new(),
        used: 0,
    });
    reader.config_mut().expand_empty_elements = true;
    let mut buffer = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut text = String::new();
    let mut root = false;
    let mut ended = false;
    let mut entry = RawEntry::default();
    let mut props = Props::default();
    let mut prop_status = None;
    let mut response_status = None;
    let mut entry_bytes = 0;
    loop {
        reader.get_mut().used = 0;
        let (namespace, event) =
            tokio::time::timeout(timeout, reader.read_resolved_event_into_async(&mut buffer))
                .await
                .map_err(|_| anyhow::anyhow!("WebDAV XML read timed out"))?
                .map_err(|error| match error {
                    quick_xml::Error::Io(error) if error.kind() != io::ErrorKind::InvalidData => {
                        anyhow::anyhow!("WebDAV response stream failed")
                    }
                    _ => invalid_xml(),
                })?;
        let dav = namespace == ResolveResult::Bound(Namespace(b"DAV:"));
        if stack.len() >= 2 {
            entry_bytes += event.as_ref().len();
            if entry_bytes > XML_TOKEN {
                return Err(permanent("WebDAV XML resource exceeds limit"));
            }
        }
        match event {
            Event::Start(start) => {
                if ended || stack.len() >= 32 || leaf(&stack) {
                    return Err(invalid_xml());
                }
                let name = if dav {
                    start.local_name().as_ref().to_vec()
                } else {
                    Vec::new()
                };
                if stack.is_empty() {
                    if root || name != b"multistatus" {
                        return Err(invalid_xml());
                    }
                    root = true;
                }
                stack.push(name);
                if path_is(&stack, &[b"multistatus", b"response"]) {
                    entry = RawEntry::default();
                    response_status = None;
                    entry_bytes = 0;
                }
                if path_is(&stack, &[b"multistatus", b"response", b"propstat"]) {
                    props = Props::default();
                    prop_status = None;
                }
                if path_is(
                    &stack,
                    &[
                        b"multistatus",
                        b"response",
                        b"propstat",
                        b"prop",
                        b"resourcetype",
                    ],
                ) {
                    props.directory = Some(false);
                }
                if path_is(
                    &stack,
                    &[
                        b"multistatus",
                        b"response",
                        b"propstat",
                        b"prop",
                        b"resourcetype",
                        b"collection",
                    ],
                ) {
                    props.directory = Some(true);
                }
                if leaf(&stack) {
                    text.clear();
                }
            }
            Event::Text(value) => {
                if leaf(&stack) {
                    let value = value.unescape().map_err(|_| invalid_xml())?;
                    if text.len() + value.len() > FIELD {
                        return Err(invalid_xml());
                    }
                    text.push_str(&value);
                } else if stack.is_empty() && !value.iter().all(u8::is_ascii_whitespace) {
                    return Err(invalid_xml());
                }
            }
            Event::CData(value) => {
                if !leaf(&stack) {
                    return Err(invalid_xml());
                }
                let value = std::str::from_utf8(value.as_ref()).map_err(|_| invalid_xml())?;
                if text.len() + value.len() > FIELD {
                    return Err(invalid_xml());
                }
                text.push_str(value);
            }
            Event::End(_) => {
                if stack.is_empty() {
                    return Err(invalid_xml());
                }
                if leaf(&stack) {
                    let value = std::mem::take(&mut text);
                    match stack.last().unwrap().as_slice() {
                        b"href" => {
                            if entry.href.replace(value).is_some() {
                                return Err(invalid_xml());
                            }
                        }
                        b"status" if stack.len() == 3 => response_status = Some(status(&value)?),
                        b"status" => prop_status = Some(status(&value)?),
                        b"getcontentlength" => props.size = Some(value),
                        b"getlastmodified" => props.modified = Some(value),
                        b"getetag" => props.etag = Some(value),
                        _ => {}
                    }
                }
                if path_is(&stack, &[b"multistatus", b"response", b"propstat"]) {
                    match prop_status {
                        Some(200) => entry.props.merge(std::mem::take(&mut props)),
                        Some(_) => {}
                        None => return Err(invalid_xml()),
                    }
                }
                if path_is(&stack, &[b"multistatus", b"response"]) {
                    if let Some(code) = response_status {
                        if code != 200 {
                            return Err(http_error(code));
                        }
                    }
                    emit(std::mem::take(&mut entry))?;
                    tokio::task::yield_now().await;
                }
                stack.pop();
                if stack.is_empty() {
                    ended = true;
                }
            }
            Event::DocType(_) => return Err(permanent("WebDAV XML DTDs are not allowed")),
            Event::Eof => {
                if !root || !ended || !stack.is_empty() {
                    return Err(invalid_xml());
                }
                return Ok(());
            }
            Event::Decl(declaration) => {
                if root {
                    return Err(invalid_xml());
                }
                if let Some(encoding) = declaration.encoding() {
                    let encoding = encoding.map_err(|_| invalid_xml())?;
                    if !encoding.eq_ignore_ascii_case(b"UTF-8") {
                        return Err(permanent("WebDAV XML must use UTF-8"));
                    }
                }
            }
            _ => {}
        }
        buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> Settings {
        let mut settings = Settings::new();
        Webdav.register_settings(&mut settings);
        settings
    }
    #[test]
    fn urls_paths_and_credential_precedence_do_not_inherit_an_os_user_or_leak() {
        let mut settings = settings();
        settings.set("webdav:user", "global").unwrap();
        settings.set("webdav:password", "global-secret").unwrap();
        let book: Bookmark = toml::from_str(
            "type='webdav'\nurl='https://host/files/'\nuser='book'\npassword='book-secret'",
        )
        .unwrap();
        let interactive = Password::new("interactive-secret".into());
        for (bookmark, password, expected) in [
            (None, None, "url:URL@secret"),
            (Some(&book), None, "book:book-secret"),
            (Some(&book), Some(&interactive), "book:interactive-secret"),
        ] {
            let target = Target::read(
                "davs://url:URL%40secret@host/files/%E9%9B%AA%20%25/",
                &settings,
                bookmark,
                password,
            )
            .unwrap();
            assert_eq!(target.base.as_str(), "https://host/");
            assert_eq!(target.home.display(true), "/files/雪 %/");
            assert!(target.auth.as_ref().unwrap().is_sensitive());
            assert_eq!(
                target.auth.as_ref().unwrap().to_str().unwrap(),
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(expected)
                )
            );
            assert!(!format!("{:?}", target.auth).contains("secret"));
        }
        let empty: Bookmark =
            toml::from_str("type='webdav'\nurl='http://host/'\nuser=''\npassword=''").unwrap();
        assert!(
            Target::read("http://url:secret@host/", &settings, Some(&empty), None)
                .unwrap()
                .auth
                .is_none()
        );
        assert!(Target::read("http://@host/", &settings, None, None)
            .unwrap()
            .auth
            .is_none());
        assert!(Target::read("http://host/", &settings, None, None).is_err());
        let empty_settings = self::settings();
        assert!(Target::read("dav://host/", &empty_settings, None, None)
            .unwrap()
            .auth
            .is_none());
        let location = Location::default()
            .resolve("/a/../雪 %?#/literal%2F/./file")
            .unwrap();
        let base = Url::parse("https://host/").unwrap();
        let url = location.url(&base, false);
        assert_eq!(Location::from_url(&url).unwrap(), location);
        assert!(url.query().is_none() && url.fragment().is_none());
        for url in [
            "https://host/a%2Fb",
            "https://host/a%5Cb",
            "https://host/%FF",
            "https://host/%00",
            "https://host/?secret=do-not-log",
            "https://host/#do-not-log",
        ] {
            assert!(Target::read(url, &empty_settings, None, None).is_err());
        }
        assert_eq!(
            Location::default().resolve("../../").unwrap().display(true),
            "/"
        );
    }
    #[test]
    fn http_failure_classification_and_configuration_errors_are_bounded_and_redacted() {
        for status in [401, 403, 404, 405, 409, 412, 423, 501, 507] {
            assert!(http_error(status).is::<NonRetryableError>());
            assert!(has_status(&http_error(status), &[status]));
        }
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(!http_error(status).is::<NonRetryableError>());
        }
        let mut settings = settings();
        for key in ["webdav:connect-timeout", "webdav:operation-timeout"] {
            for invalid in ["0", "301", "-1"] {
                settings.set(key, invalid).unwrap();
                assert!(seconds(&settings, key)
                    .unwrap_err()
                    .is::<NonRetryableError>());
            }
            settings.set(key, "1").unwrap();
        }
        assert!(valid_etag("\"v1\"").is_ok());
        assert!(valid_etag("W/\"v1\"").is_ok());
        assert!(valid_etag("bad\nsecret").is_err());
    }
    #[tokio::test]
    async fn xml_namespaces_failed_propstats_cdata_entities_and_chunk_boundaries() {
        let xml = "<?xml version='1.0'?><multistatus xmlns='DAV:' xmlns:e='evil'><response><href>/雪%20a&amp;b</href><propstat><prop><resourcetype/><getcontentlength>17</getcontentlength><getetag><![CDATA[\"v1\"]]></getetag><e:getetag>ignored</e:getetag></prop><status>HTTP/1.1 200 OK</status></propstat><propstat><prop><getcontentlength>not-a-number</getcontentlength></prop><status>HTTP/1.1 404 Not Found</status></propstat></response></multistatus>";
        let chunks = xml
            .as_bytes()
            .chunks(1)
            .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let mut count = 0;
        parse_xml(
            StreamBody::new(
                futures_util::stream::iter(chunks)
                    .map_ok(Frame::data)
                    .map_err(std::io::Error::into),
            )
            .boxed_unsync(),
            Duration::from_secs(3),
            &mut |entry| {
                count += 1;
                assert_eq!(entry.href.as_deref(), Some("/雪%20a&b"));
                assert_eq!(entry.props.directory, Some(false));
                assert_eq!(entry.props.size.as_deref(), Some("17"));
                assert_eq!(entry.props.etag.as_deref(), Some("\"v1\""));
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(count, 1);
    }
    #[tokio::test]
    async fn xml_rejects_dtds_unknown_entities_bad_structure_and_oversized_events() {
        let large = "x".repeat(XML_TOKEN + 1);
        let deep = format!(
            "<multistatus xmlns='DAV:'>{}{}</multistatus>",
            "<a>".repeat(40),
            "</a>".repeat(40)
        );
        for xml in ["<!DOCTYPE x SYSTEM 'http://127.0.0.1:1/secret'><multistatus xmlns='DAV:'/>", "<multistatus xmlns='evil'/>", "<multistatus xmlns='DAV:'><response><href>&unknown;</href></response></multistatus>", "<multistatus xmlns='DAV:'>", "<multistatus xmlns='DAV:'/><multistatus xmlns='DAV:'/>", "<multistatus xmlns='DAV:'><response><propstat><prop/></propstat></response></multistatus>", "<?xml version='1.0' encoding='UTF-16'?><multistatus xmlns='DAV:'/>", &deep, &format!("<multistatus xmlns='DAV:'><{large}"), &format!("<multistatus xmlns='DAV:'><response><href>{large}</href></response></multistatus>")] {
            let error = parse_xml(full_body(Bytes::from(xml.to_owned())), Duration::from_secs(3), &mut |_| Ok(())).await.unwrap_err(); assert!(error.is::<NonRetryableError>()); assert!(!format!("{error:#}").contains("127.0.0.1"));
        }
    }
    #[tokio::test]
    async fn large_xml_listing_streams_without_an_entry_count_cap_and_propagates_sink_errors() {
        let entry = "<response><href>/file</href><propstat><prop><resourcetype/><getcontentlength>12</getcontentlength></prop><status>HTTP/1.1 200 OK</status></propstat></response>";
        let xml = format!(
            "<multistatus xmlns='DAV:'>{}</multistatus>",
            entry.repeat(25000)
        );
        let mut count = 0;
        parse_xml(
            full_body(Bytes::from(xml)),
            Duration::from_secs(3),
            &mut |_| {
                count += 1;
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(count, 25000);
        let xml = format!("<multistatus xmlns='DAV:'>{entry}</multistatus>");
        let error = parse_xml(
            full_body(Bytes::from(xml)),
            Duration::from_secs(3),
            &mut |_| Err(anyhow::anyhow!("sink stopped")),
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "sink stopped");
    }
}
