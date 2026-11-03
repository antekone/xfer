/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! S3 backend, including AWS client configuration, routing, and diagnostics.
mod mkdir;
mod multipart_copy;
mod rename;
mod upload;

use crate::{
    backend::{Backend, ConnectOptions, Connection},
    core::{SettingValue, Settings},
    error::NonRetryableError,
    listing::{BucketInfo, Content, DirectoryInfo, FileInfo, ListingSink},
};
use anyhow::{bail, Result};
use async_trait::async_trait;

pub struct S3;
/// Namespace root contains buckets; all remaining components are an S3 prefix.
#[derive(Clone, Debug, Default, PartialEq)]
struct S3Location {
    bucket: Option<String>,
    prefix: String,
}
impl S3Location {
    fn path(&self) -> String {
        match &self.bucket {
            Some(bucket) => format!("/{bucket}/{}", self.prefix),
            None => "/".into(),
        }
    }
    fn resolve(&self, path: &str) -> Self {
        let absolute = path.starts_with('/');
        let base = self.path();
        let mut parts = if absolute {
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
        if parts.is_empty() {
            return Self::default();
        }
        Self {
            bucket: Some(parts[0].to_owned()),
            prefix: if parts.len() > 1 {
                format!("{}/", parts[1..].join("/"))
            } else {
                String::new()
            },
        }
    }
}

struct S3Connection {
    host: String,
    client: aws_sdk_s3::Client,
    location: S3Location,
    home: S3Location,
    redactions: Vec<String>,
}
impl S3Connection {
    fn new(client: aws_sdk_s3::Client, bucket: Option<String>, redactions: Vec<String>) -> Self {
        let location = S3Location {
            bucket,
            prefix: String::new(),
        };
        Self {
            client,
            host: String::new(),
            home: location.clone(),
            location,
            redactions,
        }
    }
}
#[async_trait]
impl Connection for S3Connection {
    fn prompt_host(&self) -> String {
        self.host.clone()
    }

    fn protocol(&self) -> &'static str {
        "s3"
    }
    async fn upload(&self, local: &std::path::Path, path: &str, overwrite: bool) -> Result<()> {
        self.upload_object(local, path, overwrite).await
    }
    async fn stat_file(&self, path: &str) -> Result<crate::transfer::RemoteFile> {
        let location = self.location.resolve(path);
        let bucket = location
            .bucket
            .as_deref()
            .ok_or_else(|| NonRetryableError("select a bucket before downloading".into()))?;
        let key = location.prefix.trim_end_matches('/');
        if key.is_empty() || directory_path(path) {
            return Err(NonRetryableError("get expects an object filename".into()).into());
        }
        let result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 404)
                {
                    NonRetryableError(format!("no such file: {path}")).into()
                } else {
                    probe_error("HeadObject", &error, &self.redactions)
                }
            })?;
        let size = result
            .content_length()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| NonRetryableError("S3 response has no valid object length".into()))?;
        Ok(crate::transfer::RemoteFile {
            size,
            revision: result.e_tag().map(str::to_owned),
        })
    }
    async fn download(
        &self,
        path: &str,
        offset: u64,
        expected: &crate::transfer::RemoteFile,
        sink: &mut dyn crate::transfer::DownloadSink,
    ) -> Result<()> {
        let location = self.location.resolve(path);
        let bucket = location
            .bucket
            .as_deref()
            .ok_or_else(|| NonRetryableError("select a bucket before downloading".into()))?;
        let key = location.prefix.trim_end_matches('/');
        if offset > expected.size {
            return Err(NonRetryableError("resume offset exceeds remote size".into()).into());
        }
        if offset == expected.size {
            return Ok(());
        }
        let mut request = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_if_match(expected.revision.clone());
        if offset > 0 {
            request = request.range(format!("bytes={offset}-"));
        }
        let mut response = request.send().await.map_err(|error| {
            if error.raw_response().is_some_and(|response| matches!(response.status().as_u16(), 404 | 412 | 416)) {
                NonRetryableError("remote file changed/disappeared or resume range is invalid; partial file was retained".into()).into()
            } else { probe_error("GetObject", &error, &self.redactions) }
        })?;
        if expected
            .revision
            .as_deref()
            .is_some_and(|revision| response.e_tag() != Some(revision))
        {
            return Err(NonRetryableError(
                "remote object revision changed; partial file was retained".into(),
            )
            .into());
        }
        if offset > 0 {
            let wanted = format!("bytes {offset}-{}/{}", expected.size - 1, expected.size);
            if response.content_range() != Some(wanted.as_str()) {
                return Err(NonRetryableError(
                    "server did not honor the resume range; refusing to append a full response"
                        .into(),
                )
                .into());
            }
        }
        if response
            .content_length()
            .is_some_and(|length| u64::try_from(length).ok() != Some(expected.size - offset))
        {
            return Err(NonRetryableError(
                "S3 response length does not match the requested download".into(),
            )
            .into());
        }
        let mut position = offset;
        while let Some(chunk) =
            tokio::time::timeout(std::time::Duration::from_secs(30), response.body.next())
                .await
                .map_err(|_| anyhow::anyhow!("download stalled for 30 seconds"))?
        {
            let chunk = chunk.map_err(|_| anyhow::anyhow!("download stream interrupted"))?;
            if chunk.len() as u64 > expected.size - position {
                return Err(NonRetryableError("server sent more data than expected".into()).into());
            }
            sink.write(&chunk)?;
            position += chunk.len() as u64;
        }
        if position != expected.size {
            bail!("download ended early at {position}/{} bytes", expected.size);
        }
        Ok(())
    }
    fn resolve_directory(&self, path: Option<&str>) -> Result<String> {
        Ok(match path {
            Some(path) => self.location.resolve(path).path(),
            None => self.home.path(),
        })
    }
    fn resolve_listing_path(&self, path: &str) -> Result<String> {
        let location = self.location.resolve(path);
        let canonical = location.path();
        if !location.prefix.is_empty() && !directory_path(path) {
            Ok(canonical.trim_end_matches('/').to_owned())
        } else {
            Ok(canonical)
        }
    }
    fn pwd(&self) -> Result<String> {
        Ok(self.location.path())
    }
    async fn select_for_removal(&self, path: &str, directory_hint: bool) -> Result<crate::walker::Selection> {
        use crate::walker::WalkSource;
        crate::backend::validate_file_path(path)?;
        let target = self.location.resolve(path);
        let bucket = target.bucket.as_deref().ok_or_else(|| NonRetryableError("cannot remove S3 buckets".into()))?;
        if target.prefix.is_empty() { return Err(NonRetryableError("cannot remove an S3 bucket".into()).into()); }
        let selection_path = if directory_hint { format!("{}/", path.trim_end_matches('/')) } else { path.to_owned() };
        let selection = crate::walker::RemoteSource(self).select(&selection_path).await?;
        if matches!(selection, crate::walker::Selection::Directory(_)) {
            let probe = self.client.list_objects_v2().bucket(bucket).prefix(&target.prefix).max_keys(1).send().await
                .map_err(|error| operation_error("ListObjectsV2", &error, &self.redactions))?;
            if probe.contents().is_empty() { return Err(NonRetryableError("no such S3 directory".into()).into()); }
        }
        Ok(selection)
    }
    async fn remove_empty_directory(&self, path: &str) -> Result<()> {
        crate::backend::validate_file_path(path)?;
        let target = self.location.resolve(path);
        let bucket = target.bucket.as_deref().ok_or_else(|| NonRetryableError("cannot remove S3 buckets".into()))?;
        if target.prefix.is_empty() { return Err(NonRetryableError("cannot remove an S3 bucket".into()).into()); }
        let result = self.client.list_objects_v2().bucket(bucket).prefix(&target.prefix).max_keys(2).send().await
            .map_err(|error| operation_error("ListObjectsV2", &error, &self.redactions))?;
        if result.is_truncated().unwrap_or(false) || result.contents().iter().any(|object| object.key() != Some(target.prefix.as_str())) {
            return Err(NonRetryableError("S3 directory is not empty".into()).into());
        }
        if let Some(marker) = result.contents().first() {
            let tag = marker.e_tag().filter(|tag| rename::strong_tag(tag))
                .ok_or_else(|| NonRetryableError("S3 rm requires a strong directory marker ETag".into()))?;
            self.client.delete_object().bucket(bucket).key(&target.prefix).if_match(tag).send().await
                .map_err(|error| operation_error("DeleteObject", &error, &self.redactions))?;
        }
        Ok(())
    }
    async fn remove_file(&self, path: &str) -> Result<()> {
        crate::backend::validate_file_path(path)?;
        let target = self.location.resolve(path);
        let bucket = target.bucket.as_deref().ok_or_else(|| NonRetryableError("select an S3 bucket before deleting objects".into()))?;
        let key = target.prefix.trim_end_matches('/');
        if key.is_empty() { return Err(NonRetryableError("cannot remove an S3 bucket".into()).into()); }
        let before = self.client.head_object().bucket(bucket).key(key).send().await
            .map_err(|error| operation_error("HeadObject",&error,&self.redactions))?;
        let tag = before.e_tag().filter(|tag| rename::strong_tag(tag))
            .ok_or_else(|| NonRetryableError("S3 rm requires a strong quoted ETag".into()))?;
        self.client.delete_object().bucket(bucket).key(key).if_match(tag).send().await
            .map_err(|error| operation_error("DeleteObject",&error,&self.redactions))?;
        Ok(())
    }
    async fn rename_file(&self, source: &str, destination: &str, overwrite: bool) -> Result<()> {
        self.move_object(source, destination, overwrite).await
    }
    async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        self.mkdir(path, parents).await
    }
    async fn change_directory(&mut self, path: &str) -> Result<()> {
        let target = self.location.resolve(path);
        if target == self.location {
            return Ok(());
        }
        match &target.bucket {
            None => {
                self.client
                    .list_buckets()
                    .max_buckets(1)
                    .send()
                    .await
                    .map_err(|error| probe_error("ListBuckets", &error, &self.redactions))?;
            }
            Some(bucket) if target.prefix.is_empty() => {
                self.client
                    .head_bucket()
                    .bucket(bucket)
                    .send()
                    .await
                    .map_err(|error| {
                        if error
                            .raw_response()
                            .is_some_and(|response| response.status().as_u16() == 404)
                        {
                            anyhow::Error::from(NonRetryableError(format!(
                                "no such bucket: {bucket}"
                            )))
                        } else {
                            probe_error("HeadBucket", &error, &self.redactions)
                        }
                    })?;
            }
            Some(bucket) => {
                // Prefixes are virtual: existence means at least one object,
                // including a directory marker. Do not download object data.
                let result = self
                    .client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(&target.prefix)
                    .max_keys(1)
                    .send()
                    .await
                    .map_err(|error| {
                        if error
                            .raw_response()
                            .is_some_and(|response| response.status().as_u16() == 404)
                        {
                            anyhow::Error::from(NonRetryableError(format!(
                                "no such directory: {}",
                                target.path()
                            )))
                        } else {
                            probe_error("ListObjectsV2", &error, &self.redactions)
                        }
                    })?;
                if result.contents().is_empty() && result.common_prefixes().is_empty() {
                    return Err(
                        NonRetryableError(format!("no such directory: {}", target.path())).into(),
                    );
                }
            }
        }
        // Only commit after successful validation; failures/cancellation retain cwd.
        self.location = target;
        Ok(())
    }
    async fn list(&self, path: &str, output: &mut dyn ListingSink) -> Result<u64> {
        let location = self.location.resolve(path);
        let prefix = &location.prefix;
        let mut count = 0;
        let mut token = None;
        if let Some(bucket) = &location.bucket {
            if !prefix.is_empty() && !directory_path(path) {
                let key = prefix.trim_end_matches('/');
                // A literal file wins over an equally named virtual directory.
                // ListObjects gives metadata without requiring GetObject/HEAD access.
                let probe = self
                    .client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(key)
                    .max_keys(1)
                    .send()
                    .await
                    .map_err(|error| probe_error("ListObjectsV2", &error, &self.redactions))?;
                if let Some(object) = probe
                    .contents()
                    .iter()
                    .find(|object| object.key() == Some(key))
                {
                    output.emit_file(object_content(
                        object,
                        key.rsplit('/').next().unwrap_or(key),
                    ))?;
                    return Ok(1);
                }
            }
            loop {
                let page = self
                    .client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix)
                    .delimiter("/")
                    .set_continuation_token(token)
                    .max_keys(1000)
                    .send()
                    .await
                    .map_err(|error| probe_error("ListObjectsV2", &error, &self.redactions))?;
                for common_prefix in page.common_prefixes() {
                    if let Some(name) = common_prefix.prefix() {
                        output.emit(Content::Directory(DirectoryInfo {
                            unix: None,
                            size: None,
                            name: name.strip_prefix(prefix).unwrap_or(name).to_owned(),
                            modified: None,
                        }))?;
                        count += 1;
                    }
                }
                for object in page.contents() {
                    if let Some(name) = object.key() {
                        // An optional zero-byte directory marker is the current
                        // directory itself, not one of its children.
                        if name == prefix && name.ends_with('/') {
                            continue;
                        }
                        output.emit(object_content(
                            object,
                            name.strip_prefix(prefix).unwrap_or(name),
                        ))?;
                        count += 1;
                    }
                }
                token = page
                    .next_continuation_token()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                if token.is_none() {
                    break;
                }
            }
        } else {
            loop {
                let page = self
                    .client
                    .list_buckets()
                    .set_continuation_token(token)
                    .max_buckets(1000)
                    .send()
                    .await
                    .map_err(|error| probe_error("ListBuckets", &error, &self.redactions))?;
                for bucket in page.buckets() {
                    if let Some(name) = bucket.name() {
                        output.emit(Content::Bucket(BucketInfo {
                            name: name.to_owned(),
                            created: bucket.creation_date().and_then(|value| {
                                chrono::DateTime::from_timestamp(value.secs(), value.subsec_nanos())
                            }),
                        }))?;
                        count += 1;
                    }
                }
                token = page
                    .continuation_token()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                if token.is_none() {
                    break;
                }
            }
        }
        Ok(count)
    }
}

fn directory_path(path: &str) -> bool {
    path.is_empty() || path.ends_with('/') || matches!(path.rsplit('/').next(), Some("." | ".."))
}
fn object_content(object: &aws_sdk_s3::types::Object, name: &str) -> Content {
    Content::File(FileInfo {
        unix: None,
        name: name.to_owned(),
        size: object.size().and_then(|value| u64::try_from(value).ok()),
        modified: object
            .last_modified()
            .and_then(|value| chrono::DateTime::from_timestamp(value.secs(), value.subsec_nanos())),
    })
}

#[async_trait]
impl Backend for S3 {
    fn protocol(&self) -> &'static str {
        "s3"
    }
    fn register_settings(&self, settings: &mut Settings) {
        settings.register(
            "s3:region",
            SettingValue::Text("us-east-1".into()),
            "Default S3 signing region",
        );
        settings.register(
            "s3:endpoint",
            SettingValue::Text(String::new()),
            "Optional S3-compatible HTTP(S) endpoint",
        );
    }
    async fn connect(&self, options: ConnectOptions) -> Result<Box<dyn Connection>> {
        let skip_tls_validation = options.settings.bool("ssl:skip-validation");
        let mut redactions = Vec::new();
        if let Some(bookmark) = &options.bookmark {
            if let Ok(url) = url::Url::parse(&bookmark.url) {
                if !url.username().is_empty() || url.password().is_some() || url.query().is_some() {
                    redactions.push(bookmark.url.clone());
                    if let Some(password) = url.password() {
                        redactions.push(password.to_owned());
                    }
                    for (_, value) in url.query_pairs() {
                        redactions.push(value.into_owned());
                    }
                }
            }
            redactions.extend(
                [
                    &bookmark.access_key,
                    &bookmark.secret_key,
                    &bookmark.session_token,
                ]
                .into_iter()
                .filter_map(|value| value.clone()),
            );
        }
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            if let Ok(value) = std::env::var(key) {
                redactions.push(value);
            }
        }
        let mut override_host = None;
        let mut override_port = None;
        let mut bucket = None;
        let mut endpoint = options
            .settings
            .text("s3:endpoint")
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let mut region = options
            .settings
            .text("s3:region")
            .unwrap_or("us-east-1")
            .to_owned();
        let mut credentials = None;
        if let Some(bookmark) = options.bookmark {
            override_host = bookmark.override_host;
            override_port = bookmark.override_port;
            if bookmark.api.as_deref().is_some_and(|api| api != "s3v4") {
                bail!("only api = 's3v4' is supported");
            }
            if let Some(value) = bookmark.region {
                region = value;
            }
            if bookmark.url.starts_with("s3://") {
                // A bucket URL is not an HTTP endpoint: leave endpoint unset
                // unless explicitly configured, so AWS resolves regional HTTPS.
                bucket = Some(parse_bucket(&bookmark.url)?);
            } else {
                validate_endpoint(&bookmark.url)?;
                endpoint = Some(bookmark.url);
            }
            if bookmark.bucket.is_some() {
                bucket = bookmark.bucket;
            }
            if let (Some(key), Some(secret)) = (bookmark.access_key, bookmark.secret_key) {
                credentials = Some(aws_sdk_s3::config::Credentials::new(
                    key,
                    secret,
                    bookmark.session_token,
                    None,
                    "xfer-bookmark",
                ));
            }
        } else {
            bucket = Some(parse_bucket(&options.target)?);
        }
        if let Some(endpoint) = &endpoint {
            validate_endpoint(endpoint)?;
        }
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region))
            .timeout_config(
                aws_config::timeout::TimeoutConfig::builder()
                    .operation_timeout(std::time::Duration::from_secs(30))
                    .build(),
            );
        if let Some(credentials) = credentials {
            loader = loader.credentials_provider(credentials);
        }
        let shared = loader.load().await;
        let custom_http =
            if skip_tls_validation || override_host.is_some() || override_port.is_some() {
                Some(s3_http_client(
                    skip_tls_validation,
                    override_host.as_deref(),
                    override_port,
                    endpoint.as_deref(),
                )?)
            } else {
                None
            };
        // The daemon owns retries/backoff and cancellation, not a second SDK loop.
        let mut config = aws_sdk_s3::config::Builder::from(&shared)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1));
        let host = endpoint
            .as_deref()
            .and_then(|value| url::Url::parse(value).ok())
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_default();
        if let Some(endpoint) = endpoint {
            config = config.endpoint_url(endpoint).force_path_style(true);
        }
        if let Some(http_client) = custom_http {
            config = config.http_client(http_client);
        }
        let client = aws_sdk_s3::Client::from_conf(config.build());
        // S3 has no login handshake: probe the bucket or account to validate access.
        // Do not expose SDK debug output (which can contain request/credential details).
        if let Some(bucket) = &bucket {
            client
                .head_bucket()
                .bucket(bucket)
                .send()
                .await
                .map_err(|error| probe_error("HeadBucket", &error, &redactions))?;
        } else {
            client
                .list_buckets()
                .send()
                .await
                .map_err(|error| probe_error("ListBuckets", &error, &redactions))?;
        }
        let mut connection = S3Connection::new(client, bucket, redactions);
        connection.host = host;
        Ok(Box::new(connection))
    }
}

/// Deliberately insecure, installed only for a job explicitly configured with
/// ssl:skip-validation=true. This bypasses certificate trust/name checks, not TLS
/// handshake signature verification or encryption. Never changes global TLS state.
#[derive(Debug)]
struct SkipCertificateValidation;
impl rustls::client::ServerCertVerifier for SkipCertificateValidation {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

/// This connector changes only where TCP connects. Hyper and rustls retain the
/// original request URI for Host, SigV4, certificate verification and TLS SNI.
#[derive(Clone)]
struct RouteConnector {
    inner: hyper::client::HttpConnector,
    authority: Option<hyper::http::uri::Authority>,
}
impl tower_service::Service<hyper::Uri> for RouteConnector {
    type Response = <hyper::client::HttpConnector as tower_service::Service<hyper::Uri>>::Response;
    type Error = <hyper::client::HttpConnector as tower_service::Service<hyper::Uri>>::Error;
    type Future = <hyper::client::HttpConnector as tower_service::Service<hyper::Uri>>::Future;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, destination: hyper::Uri) -> Self::Future {
        let destination = if let Some(authority) = &self.authority {
            let mut parts = destination.into_parts();
            parts.authority = Some(authority.clone());
            hyper::Uri::from_parts(parts).expect("validated TCP destination authority")
        } else {
            destination
        };
        self.inner.call(destination)
    }
}

fn s3_http_client(
    skip_validation: bool,
    override_host: Option<&str>,
    override_port: Option<u16>,
    endpoint: Option<&str>,
) -> Result<aws_sdk_s3::config::SharedHttpClient> {
    let authority = match override_host {
        Some(host) => {
            let endpoint = url::Url::parse(endpoint.ok_or_else(|| {
                anyhow::anyhow!("override_host requires an HTTP(S) S3 endpoint")
            })?)?;
            let host = url::Host::parse(host).map_err(|_| {
                anyhow::anyhow!(
                    "override_host must be a hostname or IP address, without scheme or port"
                )
            })?;
            let port = override_port
                .or_else(|| endpoint.port_or_known_default())
                .ok_or_else(|| anyhow::anyhow!("endpoint has no TCP port"))?;
            if port == 0 {
                bail!("override_port must be between 1 and 65535");
            }
            Some(format!("{host}:{port}").parse::<hyper::http::uri::Authority>()?)
        }
        None if override_port.is_some() => bail!("override_port requires override_host"),
        None => None,
    };
    let builder = hyper_rustls::HttpsConnectorBuilder::new();
    let builder = if skip_validation {
        let tls = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(std::sync::Arc::new(SkipCertificateValidation))
            .with_no_client_auth();
        builder.with_tls_config(tls)
    } else {
        builder.with_native_roots()
    };
    let mut tcp = hyper::client::HttpConnector::new();
    tcp.enforce_http(false);
    let connector = builder
        .https_or_http()
        .enable_http1()
        .wrap_connector(RouteConnector {
            inner: tcp,
            authority,
        });
    Ok(aws_smithy_http_client::hyper_014::HyperClientBuilder::new().build(connector))
}

fn diagnostic_text(text: &str, redactions: &[String]) -> String {
    let mut text = text.to_owned();
    // Replace longer values first in case one credential is a prefix of another.
    let mut secrets = redactions
        .iter()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
    for secret in secrets {
        text = text.replace(secret.as_str(), "[redacted]");
    }
    text.chars()
        .take(2048)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn probe_error<E>(
    operation: &str,
    error: &aws_sdk_s3::error::SdkError<E>,
    redactions: &[String],
) -> anyhow::Error
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    use std::error::Error;
    let mut details = vec![format!("S3 {operation} failed")];
    if let Some(response) = error.raw_response() {
        details.push(format!("HTTP {}", response.status().as_u16()));
        for (header, label) in [
            ("x-amz-bucket-region", "bucket region"),
            ("x-amz-request-id", "request ID"),
        ] {
            if let Some(value) = response.headers().get(header) {
                details.push(format!("{label}: {}", diagnostic_text(value, redactions)));
            }
        }
        match response.status().as_u16() {
            301 | 307 => details.push("check endpoint and signing region".into()),
            403 => details.push("access denied; check credentials and bucket permissions".into()),
            404 => details.push("bucket not found; check bucket name and endpoint".into()),
            _ => {}
        }
    }
    if let Some(service) = error.as_service_error() {
        if let Some(code) = service.code() {
            details.push(diagnostic_text(code, redactions));
        }
        if let Some(message) = service.message() {
            details.push(diagnostic_text(message, redactions));
        }
    } else {
        details.push(diagnostic_text(&error.to_string(), redactions));
        let mut source = error.source();
        for _ in 0..8 {
            let Some(cause) = source else {
                break;
            };
            let message = diagnostic_text(&cause.to_string(), redactions);
            if !details.contains(&message) {
                details.push(message);
            }
            source = cause.source();
        }
    }
    anyhow::anyhow!(details.join("; "))
}

fn operation_error<E>(
    operation: &str,
    error: &aws_sdk_s3::error::SdkError<E>,
    redactions: &[String],
) -> anyhow::Error
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let diagnostic = probe_error(operation, error, redactions);
    let code = error.as_service_error().and_then(|service| service.code());
    let transient = matches!(
        code,
        Some(
            "RequestTimeout"
                | "RequestTimeoutException"
                | "SlowDown"
                | "Throttling"
                | "ThrottlingException"
                | "RequestLimitExceeded"
        )
    );
    let permanent = error.raw_response().is_some_and(|response| {
        let status = response.status().as_u16();
        ((300..500).contains(&status) && !matches!(status, 408 | 409 | 425 | 429))
            || matches!(status, 501 | 505)
    });
    if permanent && !transient {
        NonRetryableError(diagnostic.to_string()).into()
    } else {
        diagnostic
    }
}

fn validate_endpoint(value: &str) -> Result<()> {
    if !(value.starts_with("https://") || value.starts_with("http://")) {
        bail!("S3 endpoint must use http:// or https://");
    }
    Ok(())
}
fn parse_bucket(target: &str) -> Result<String> {
    let bucket = target.strip_prefix("s3://").unwrap_or("");
    if bucket.is_empty() || bucket.contains(['/', '?', '#', '@', ':']) {
        bail!("expected s3://bucket (no object key for connect)");
    }
    Ok(bucket.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn errors_include_causes_but_redact_credentials_and_control_characters() {
        let error = aws_sdk_s3::error::SdkError::<
            aws_sdk_s3::operation::head_bucket::HeadBucketError,
        >::construction_failure(std::io::Error::other(
            "TLS certificate rejected; credential=secret\nnext line",
        ));
        let message = probe_error("HeadBucket", &error, &["secret".into()]).to_string();
        assert!(message.contains("TLS certificate rejected"));
        assert!(message.contains("[redacted]"));
        assert!(!message.contains("secret"));
        assert!(!message.contains('\n'));
    }

    #[test]
    fn s3_directory_resolution_handles_root_parents_and_relative_paths() {
        let root = S3Location::default();
        assert_eq!(root.path(), "/");
        let directory = root.resolve("archive/photos/2026");
        assert_eq!(directory.path(), "/archive/photos/2026/");
        assert_eq!(
            directory.resolve("../other").path(),
            "/archive/photos/other/"
        );
        assert_eq!(directory.resolve("./").path(), directory.path());
        assert_eq!(directory.resolve("/other/images").path(), "/other/images/");
        assert_eq!(directory.resolve("../../..").path(), "/");
        assert_eq!(directory.resolve("../../../../..").path(), "/");
        assert_eq!(
            root.resolve("//archive///photos//").path(),
            "/archive/photos/"
        );
    }

    #[tokio::test]
    async fn bucket_bookmark_defaults_to_https_but_explicit_http_is_respected() {
        use aws_sdk_s3::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
        let resolver = DefaultResolver::new();
        let bucket = parse_bucket("s3://example").unwrap();
        let params = Params::builder().region("us-east-1").bucket(bucket.clone()).build().unwrap();
        let endpoint = ResolveEndpoint::resolve_endpoint(&resolver,&params).await.unwrap();
        assert!(endpoint.url().starts_with("https://"),"{}",endpoint.url());
        let params = Params::builder().region("us-east-1").bucket(bucket).endpoint("http://localhost:9000").force_path_style(true).build().unwrap();
        let endpoint = ResolveEndpoint::resolve_endpoint(&resolver,&params).await.unwrap();
        assert!(endpoint.url().starts_with("http://"),"{}",endpoint.url());
    }

    #[test]
    fn bucket_urls() {
        assert_eq!(parse_bucket("s3://example").unwrap(), "example");
        for url in ["s3://", "s3://example/key", "https://example", "s3://a?x"] {
            assert!(parse_bucket(url).is_err());
        }
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::bookmarks::Bookmark;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
    };

    #[derive(Default)]
    struct Records(Vec<Content>);
    impl ListingSink for Records {
        fn emit(&mut self, content: Content) -> Result<()> {
            self.0.push(content);
            Ok(())
        }
    }

    fn self_signed_endpoint() -> (String, tokio::sync::oneshot::Receiver<bool>) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::Certificate(certified.cert.der().to_vec())],
                rustls::PrivateKey(certified.key_pair.serialize_der()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let tls = rustls::ServerConnection::new(std::sync::Arc::new(tls)).unwrap();
            let stream = rustls::StreamOwned::new(tls, socket);
            let mut reader = BufReader::new(stream);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => {
                        let _ = send.send(false);
                        return;
                    }
                    Ok(_) if line == "\r\n" => break,
                    _ => {}
                }
            }
            let _ = reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            let _ = reader.get_mut().flush();
            let _ = send.send(true);
        });
        (format!("https://{address}"), receive)
    }

    #[tokio::test]
    async fn insecure_tls_is_opt_in_and_accepts_self_signed_endpoint() {
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .load()
            .await;
        let (endpoint, received) = self_signed_endpoint();
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .build();
        assert!(aws_sdk_s3::Client::from_conf(config)
            .head_bucket()
            .bucket("example")
            .send()
            .await
            .is_err());
        assert!(!received.await.unwrap());

        let (endpoint, received) = self_signed_endpoint();
        let mut settings = Settings::new();
        S3.register_settings(&mut settings);
        settings.register("ssl:skip-validation", SettingValue::Bool(true), "test");
        let bookmark = Bookmark {
            protocol: "s3".into(),
            url: endpoint,
            access_key: Some("test".into()),
            secret_key: Some("secret".into()),
            session_token: None,
            region: None,
            bucket: Some("example".into()),
            api: None,
            override_host: None,
            override_port: None,
            user: None,
            domain: None,
            password: None,
            password_interactive: false,
        };
        let connection = S3
            .connect(ConnectOptions {
                target: "test".into(),
                bookmark: Some(bookmark),
                settings,
                password: None,
            })
            .await
            .unwrap();
        assert!(received.await.unwrap());
        connection.disconnect().await.unwrap();
    }

    async fn listing_client(
        pages: Vec<&'static str>,
    ) -> (
        aws_sdk_s3::Client,
        tokio::sync::oneshot::Receiver<Vec<String>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut requests = Vec::new();
            for xml in pages {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut first = String::new();
                reader.read_line(&mut first).unwrap();
                requests.push(first);
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                }
                write!(socket, "HTTP/1.1 200 OK\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", xml.len(), xml).unwrap();
            }
            let _ = send.send(requests);
        });
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(format!("http://{address}"))
            .force_path_style(true)
            .build();
        (aws_sdk_s3::Client::from_conf(config), receive)
    }

    #[tokio::test]
    async fn lists_all_bucket_pages_as_structured_records() {
        let (client, requests) = listing_client(vec![
            "<ListAllMyBucketsResult><Buckets><Bucket><Name>alpha</Name><CreationDate>2025-04-16T10:11:00Z</CreationDate></Bucket></Buckets><ContinuationToken>next</ContinuationToken></ListAllMyBucketsResult>",
            "<ListAllMyBucketsResult><Buckets><Bucket><Name>beta</Name></Bucket></Buckets></ListAllMyBucketsResult>",
        ]).await;
        let connection = S3Connection::new(client, None, Vec::new());
        let mut records = Records::default();
        assert_eq!(connection.list("", &mut records).await.unwrap(), 2);
        let date = chrono::DateTime::parse_from_rfc3339("2025-04-16T10:11:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            records.0,
            vec![
                Content::Bucket(BucketInfo {
                    name: "alpha".into(),
                    created: Some(date)
                }),
                Content::Bucket(BucketInfo {
                    name: "beta".into(),
                    created: None
                }),
            ]
        );
        assert!(requests.await.unwrap()[1].contains("continuation-token=next"));
    }

    #[tokio::test]
    async fn lists_objects_and_prefixes_across_pages() {
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><IsTruncated>true</IsTruncated><CommonPrefixes><Prefix>photos/sub/</Prefix></CommonPrefixes><NextContinuationToken>next</NextContinuationToken></ListBucketResult>",
            "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>photos/a.jpg</Key><Size>42</Size><LastModified>2026-10-16T13:22:00Z</LastModified></Contents></ListBucketResult>",
        ]).await;
        let connection = S3Connection::new(client, Some("example".into()), Vec::new());
        let mut records = Records::default();
        assert_eq!(connection.list("photos/", &mut records).await.unwrap(), 2);
        let date = chrono::DateTime::parse_from_rfc3339("2026-10-16T13:22:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            records.0,
            vec![
                Content::Directory(DirectoryInfo {
                    unix: None,
                    size: None,
                    name: "sub/".into(),
                    modified: None
                }),
                Content::File(FileInfo {
                    unix: None,
                    name: "a.jpg".into(),
                    size: Some(42),
                    modified: Some(date)
                }),
            ]
        );
        let requests = requests.await.unwrap();
        assert!(
            requests[0].starts_with("GET /example/") || requests[0].starts_with("GET /example?"),
            "{}",
            requests[0]
        );
        assert!(requests[1].contains("continuation-token=next"));
    }

    async fn download_client(
        responses: Vec<(&'static str, &'static [u8])>,
    ) -> (S3Connection, tokio::sync::oneshot::Receiver<Vec<String>>) {
        http_client(
            responses
                .into_iter()
                .map(|(headers, body)| (headers.into(), body.into()))
                .collect(),
        )
        .await
    }

    pub(super) async fn http_client(
        responses: Vec<(String, Vec<u8>)>,
    ) -> (S3Connection, tokio::sync::oneshot::Receiver<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (headers, body) in responses {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                        break;
                    }
                    request.push_str(&line);
                }
                // Consume upload bodies before replying, including any SDK
                // checksum framing (whose encoded length is advertised).
                let length = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                }).unwrap_or(0);
                let mut body_bytes = vec![0; length];
                std::io::Read::read_exact(&mut reader, &mut body_bytes).unwrap();
                request.push_str(&String::from_utf8_lossy(&body_bytes));
                requests.push(request);
                socket.write_all(headers.as_bytes()).unwrap();
                socket.write_all(&body).unwrap();
            }
            let _ = send.send(requests);
        });
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(format!("http://{address}"))
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .build();
        (
            S3Connection::new(
                aws_sdk_s3::Client::from_conf(config),
                Some("example".into()),
                Vec::new(),
            ),
            receive,
        )
    }
    #[derive(Default)]
    struct Downloaded(Vec<u8>);
    impl crate::transfer::DownloadSink for Downloaded {
        fn write(&mut self, bytes: &[u8]) -> Result<()> {
            self.0.extend_from_slice(bytes);
            Ok(())
        }
    }

    #[tokio::test]
    async fn s3_download_supports_head_metadata_if_match_and_ranges() {
        let (connection, requests) = download_client(vec![
            ("HTTP/1.1 200 OK\r\ncontent-length: 6\r\netag: \"v1\"\r\nconnection: close\r\n\r\n", b""),
            ("HTTP/1.1 200 OK\r\ncontent-length: 6\r\netag: \"v1\"\r\nconnection: close\r\n\r\n", b"abcdef"),
            ("HTTP/1.1 206 Partial Content\r\ncontent-length: 3\r\ncontent-range: bytes 3-5/6\r\netag: \"v1\"\r\nconnection: close\r\n\r\n", b"def"),
        ]).await;
        let metadata = connection.stat_file("/example/a.txt").await.unwrap();
        assert_eq!(metadata.size, 6);
        assert_eq!(metadata.revision.as_deref(), Some("\"v1\""));
        let mut full = Downloaded::default();
        connection
            .download("/example/a.txt", 0, &metadata, &mut full)
            .await
            .unwrap();
        assert_eq!(full.0, b"abcdef");
        let mut tail = Downloaded::default();
        connection
            .download("/example/a.txt", 3, &metadata, &mut tail)
            .await
            .unwrap();
        assert_eq!(tail.0, b"def");
        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("HEAD /example/a.txt"));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("if-match: \"v1\""));
        assert!(requests[2].to_ascii_lowercase().contains("range: bytes=3-"));
    }
    #[tokio::test]
    async fn changed_revision_is_rejected_before_writing_any_bytes() {
        let (connection, requests) = download_client(vec![
            ("HTTP/1.1 206 Partial Content\r\ncontent-length: 3\r\ncontent-range: bytes 3-5/6\r\netag: \"v2\"\r\nconnection: close\r\n\r\n", b"xyz"),
        ]).await;
        let metadata = crate::transfer::RemoteFile {
            size: 6,
            revision: Some("\"v1\"".into()),
        };
        let mut output = Downloaded::default();
        assert!(connection
            .download("/example/a.txt", 3, &metadata, &mut output)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        assert!(output.0.is_empty());
        requests.await.unwrap();
    }

    #[tokio::test]
    async fn ignored_resume_range_cannot_append_duplicate_data() {
        let (connection, requests) = download_client(vec![(
            "HTTP/1.1 200 OK\r\ncontent-length: 6\r\netag: \"v1\"\r\nconnection: close\r\n\r\n",
            b"abcdef",
        )])
        .await;
        let metadata = crate::transfer::RemoteFile {
            size: 6,
            revision: Some("\"v1\"".into()),
        };
        let mut output = Downloaded::default();
        assert!(connection
            .download("/example/a.txt", 3, &metadata, &mut output)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        assert!(output.0.is_empty());
        requests.await.unwrap();
    }

    #[tokio::test]
    async fn ls_file_uses_listing_metadata_and_does_not_change_pwd() {
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><Contents><Key>notes.txt</Key><Size>42</Size><LastModified>2026-10-16T13:22:00Z</LastModified></Contents></ListBucketResult>",
        ]).await;
        let connection = S3Connection::new(client, Some("example".into()), Vec::new());
        let target = connection.resolve_listing_path("notes.txt").unwrap();
        assert_eq!(target, "/example/notes.txt");
        let mut records = Records::default();
        assert_eq!(connection.list(&target, &mut records).await.unwrap(), 1);
        assert!(
            matches!(&records.0[0], Content::File(info) if info.name == "notes.txt" && info.size == Some(42) && info.modified.is_some())
        );
        assert_eq!(connection.pwd().unwrap(), "/example/");
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /example"));
        assert!(requests[0].contains("prefix=notes.txt"));
        assert!(requests[0].contains("max-keys=1"));
    }

    #[tokio::test]
    async fn ls_bare_directory_falls_back_to_prefix_listing() {
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><Contents><Key>photos/a.txt</Key><Size>42</Size></Contents></ListBucketResult>",
            "<ListBucketResult><Contents><Key>photos/a.txt</Key><Size>42</Size></Contents></ListBucketResult>",
        ]).await;
        let connection = S3Connection::new(client, Some("example".into()), Vec::new());
        let target = connection.resolve_listing_path("photos").unwrap();
        assert_eq!(target, "/example/photos");
        let mut records = Records::default();
        assert_eq!(connection.list(&target, &mut records).await.unwrap(), 1);
        assert!(matches!(&records.0[0], Content::File(info) if info.name == "a.txt"));
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains("prefix=photos%2F"));
    }

    #[tokio::test]
    async fn ls_file_and_equally_named_directory_are_unambiguous() {
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><Contents><Key>foo</Key><Size>10</Size></Contents></ListBucketResult>",
            "<ListBucketResult><Contents><Key>foo/a.txt</Key><Size>42</Size></Contents></ListBucketResult>",
        ]).await;
        let connection = S3Connection::new(client, Some("example".into()), Vec::new());
        let mut file = Records::default();
        connection.list("foo", &mut file).await.unwrap();
        assert!(matches!(&file.0[0], Content::File(info) if info.name == "foo"));
        let mut folder = Records::default();
        connection.list("foo/", &mut folder).await.unwrap();
        assert!(matches!(&folder.0[0], Content::File(info) if info.name == "a.txt"));
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn directory_changes_validate_existence_and_list_relative_names() {
        let (client, requests) = listing_client(vec![
            "", // HeadBucket for cd example
            "<ListBucketResult><Contents><Key>photos/a.txt</Key></Contents></ListBucketResult>",
            "<ListBucketResult><CommonPrefixes><Prefix>photos/sub/</Prefix></CommonPrefixes><Contents><Key>photos/</Key><Size>0</Size></Contents><Contents><Key>photos/a.txt</Key><Size>42</Size></Contents></ListBucketResult>",
            "<ListBucketResult></ListBucketResult>", // cd missing must not change cwd
            "", // HeadBucket for cd ..
            "<ListAllMyBucketsResult><Buckets><Bucket><Name>example</Name></Bucket></Buckets></ListAllMyBucketsResult>",
        ]).await;
        let mut connection = S3Connection::new(client, None, Vec::new());
        assert_eq!(connection.pwd().unwrap(), "/");
        connection.change_directory("example").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/example/");
        connection.change_directory("photos").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/example/photos/");
        let mut records = Records::default();
        assert_eq!(connection.list("", &mut records).await.unwrap(), 2);
        assert!(matches!(&records.0[0], Content::Directory(info) if info.name == "sub/"));
        assert!(matches!(&records.0[1], Content::File(info) if info.name == "a.txt"));
        assert_eq!(connection.pwd().unwrap(), "/example/photos/"); // ls never changes cwd
        let error = connection.change_directory("../missing").await.unwrap_err();
        assert!(error.is::<NonRetryableError>());
        assert_eq!(connection.pwd().unwrap(), "/example/photos/");
        connection.change_directory("..").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/example/");
        connection.change_directory("/").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/");
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 6);
        assert!(requests[0].starts_with("HEAD /example"));
        assert!(requests[1].contains("max-keys=1"));
        assert!(requests[2].contains("prefix=photos%2F"));
        assert!(requests[3].contains("prefix=missing%2F"));
    }

    #[tokio::test]
    async fn cd_root_exits_initial_bucket_and_lists_the_account() {
        let account = "<ListAllMyBucketsResult><Buckets><Bucket><Name>ci-production</Name></Bucket><Bucket><Name>macosx</Name></Bucket></Buckets></ListAllMyBucketsResult>";
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><Contents><Key>test_agent/a.txt</Key></Contents></ListBucketResult>",
            account, // validate account root on cd /
            account, // ls must request ListBuckets, not the old bucket's objects
            "",      // HeadBucket when bare cd returns home
        ])
        .await;
        let mut connection = S3Connection::new(client, Some("macosx".into()), Vec::new());
        connection.change_directory("test_agent").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/macosx/test_agent/");
        connection.change_directory("/").await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/");
        assert_eq!(connection.location.bucket, None);
        assert!(connection.location.prefix.is_empty());
        let mut records = Records::default();
        assert_eq!(connection.list("", &mut records).await.unwrap(), 2);
        assert!(records
            .0
            .iter()
            .all(|entry| matches!(entry, Content::Bucket(_))));
        // Home is not the active directory. Only an explicit bare cd restores it.
        let home = connection.resolve_directory(None).unwrap();
        assert_eq!(home, "/macosx/");
        assert_eq!(connection.pwd().unwrap(), "/");
        connection.change_directory(&home).await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/macosx/");
        let requests = requests.await.unwrap();
        for request in &requests[1..3] {
            assert!(
                request.starts_with("GET /?"),
                "account request must not target the initial bucket: {request}"
            );
            assert!(!request.contains("list-type=2"));
            assert!(!request.contains("macosx"));
        }
        assert!(requests[3].starts_with("HEAD /macosx"));
    }

    #[tokio::test]
    async fn no_argument_cd_resolves_to_initial_bucket() {
        let (client, _) = listing_client(Vec::new()).await;
        let mut connection = S3Connection::new(client, Some("archive".into()), Vec::new());
        connection.location = connection.location.resolve("photos/2026");
        assert_eq!(connection.resolve_directory(None).unwrap(), "/archive/");
        assert_eq!(connection.resolve_directory(Some("/")).unwrap(), "/");
        assert_eq!(
            connection.resolve_directory(Some("..")).unwrap(),
            "/archive/photos/"
        );
    }

    #[tokio::test]
    async fn tcp_override_preserves_tls_name_host_and_signing() {
        let name = "minio.example.invalid";
        let cert = rcgen::generate_simple_self_signed(vec![name.into()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(&rustls::Certificate(cert.cert.der().to_vec()))
            .unwrap();
        let tls_server = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::Certificate(cert.cert.der().to_vec())],
                rustls::PrivateKey(cert.key_pair.serialize_der()),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let session = rustls::ServerConnection::new(std::sync::Arc::new(tls_server)).unwrap();
            let mut reader = BufReader::new(rustls::StreamOwned::new(session, socket));
            let mut request = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                request.push_str(&line);
            }
            let sni = reader.get_ref().conn.server_name().unwrap().to_owned();
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .unwrap();
            reader.get_mut().flush().unwrap();
            send.send((sni, request)).unwrap();
        });
        // Trust only the test CA: TLS still checks the ORIGINAL hostname.
        let tls = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let mut tcp = hyper::client::HttpConnector::new();
        tcp.enforce_http(false);
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_only()
            .enable_http1()
            .wrap_connector(RouteConnector {
                inner: tcp,
                authority: Some(format!("127.0.0.1:{port}").parse().unwrap()),
            });
        let http_client =
            aws_smithy_http_client::hyper_014::HyperClientBuilder::new().build(connector);
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(format!("https://{name}"))
            .http_client(http_client)
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(config)
            .head_bucket()
            .bucket("example")
            .send()
            .await
            .unwrap();
        let (sni, request) = receive.await.unwrap();
        assert_eq!(sni, name);
        let request = request.to_ascii_lowercase();
        assert!(request.contains("host: minio.example.invalid\r\n"));
        assert!(request.contains("authorization: aws4-hmac-sha256"));
        assert!(!request.contains("127.0.0.1"));
    }

    #[test]
    fn validates_routing_overrides() {
        assert!(s3_http_client(false, None, Some(9999), Some("https://example.com")).is_err());
        assert!(s3_http_client(
            false,
            Some("https://127.0.0.1"),
            None,
            Some("https://example.com")
        )
        .is_err());
        assert!(s3_http_client(
            false,
            Some("127.0.0.1"),
            Some(0),
            Some("https://example.com")
        )
        .is_err());
        assert!(s3_http_client(false, Some("127.0.0.1"), None, None).is_err());
        assert!(s3_http_client(
            false,
            Some("127.0.0.1"),
            Some(9999),
            Some("https://example.com")
        )
        .is_ok());
    }

    #[tokio::test]
    async fn s3_bucket_connection_is_signed_and_disconnects() {
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = server.local_addr().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let (mut socket, _) = server.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut request = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                request.push_str(&line);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .unwrap();
            let _ = send.send(request);
        });
        let mut settings = Settings::new();
        S3.register_settings(&mut settings);
        let bookmark = Bookmark {
            protocol: "s3".into(),
            url: format!("http://{address}"),
            access_key: Some("test-access-key".into()),
            secret_key: Some("test-secret-key".into()),
            session_token: None,
            region: None,
            bucket: Some("test-bucket".into()),
            api: Some("s3v4".into()),
            override_host: None,
            override_port: None,
            user: None,
            domain: None,
            password: None,
            password_interactive: false,
        };
        let connection = S3
            .connect(ConnectOptions {
                target: "test".into(),
                bookmark: Some(bookmark),
                settings,
                password: None,
            })
            .await
            .unwrap();
        assert_eq!(connection.protocol(), "s3");
        connection.disconnect().await.unwrap();
        let request = receive.await.unwrap();
        assert!(request.starts_with("HEAD /test-bucket"));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: aws4-hmac-sha256"));
        assert!(!request.contains("test-secret-key"));
    }
}
