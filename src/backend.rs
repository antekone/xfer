//! Transport-independent logical connections. No AWS types escape this module.
use crate::{
    bookmarks::Bookmark,
    core::{SettingValue, Settings},
};
use anyhow::{bail, Result};
use async_trait::async_trait;

#[derive(Clone)]
pub struct ConnectOptions {
    pub target: String,
    pub bookmark: Option<Bookmark>,
    pub settings: Settings,
}

#[async_trait]
pub trait Connection: Send + Sync {
    fn protocol(&self) -> &'static str;
    async fn list(&self, _path: &str, _output: &mut crate::core::OutputStore) -> Result<u64> {
        bail!("listing is not implemented for {}", self.protocol())
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

pub struct S3;
struct S3Connection {
    client: aws_sdk_s3::Client,
    bucket: Option<String>,
    redactions: Vec<String>,
}
#[async_trait]
impl Connection for S3Connection {
    fn protocol(&self) -> &'static str {
        "s3"
    }
    async fn list(&self, path: &str, output: &mut crate::core::OutputStore) -> Result<u64> {
        let mut count = 0;
        let mut token = None;
        if let Some(bucket) = &self.bucket {
            loop {
                let page = self
                    .client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(path)
                    .delimiter("/")
                    .set_continuation_token(token)
                    .max_keys(1000)
                    .send()
                    .await
                    .map_err(|error| probe_error("ListObjectsV2", &error, &self.redactions))?;
                for prefix in page.common_prefixes() {
                    if let Some(name) = prefix.prefix() {
                        output.write_line(&format!("DIR  {}", name.escape_default()))?;
                        count += 1;
                    }
                }
                for object in page.contents() {
                    if let Some(name) = object.key() {
                        output.write_line(&format!(
                            "{:>12}  {}",
                            object.size().unwrap_or(0),
                            name.escape_default()
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
            if !path.is_empty() {
                bail!("bucket listing takes no prefix; connect s3://bucket to list its contents");
            }
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
                        output.write_line(&name.escape_default().to_string())?;
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
        Ok(Box::new(S3Connection {
            client,
            bucket,
            redactions,
        }))
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

pub struct Sftp;
pub struct Smb;
#[async_trait]
impl Backend for Sftp {
    fn protocol(&self) -> &'static str {
        "sftp"
    }
    fn register_settings(&self, settings: &mut Settings) {
        settings.register(
            "sftp:connect-timeout",
            SettingValue::Int(30),
            "SFTP connection timeout (seconds)",
        );
    }
}
#[async_trait]
impl Backend for Smb {
    fn protocol(&self) -> &'static str {
        "smb"
    }
    fn register_settings(&self, settings: &mut Settings) {
        settings.register(
            "smb:max-credits",
            SettingValue::Int(32),
            "Maximum SMB credits",
        );
    }
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
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
    };

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
        };
        let connection = S3
            .connect(ConnectOptions {
                target: "test".into(),
                bookmark: Some(bookmark),
                settings,
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
    async fn lists_all_bucket_pages_to_spool() {
        let (client, requests) = listing_client(vec![
            "<ListAllMyBucketsResult><Buckets><Bucket><Name>alpha</Name></Bucket></Buckets><ContinuationToken>next</ContinuationToken></ListAllMyBucketsResult>",
            "<ListAllMyBucketsResult><Buckets><Bucket><Name>beta</Name></Bucket></Buckets></ListAllMyBucketsResult>",
        ]).await;
        let connection = S3Connection {
            client,
            bucket: None,
            redactions: Vec::new(),
        };
        let mut output = crate::core::OutputStore::new(9001);
        assert_eq!(connection.list("", &mut output).await.unwrap(), 2);
        assert_eq!(output.read_all(), vec!["alpha", "beta"]);
        assert!(requests.await.unwrap()[1].contains("continuation-token=next"));
        std::fs::remove_file(output.path()).unwrap();
    }

    #[tokio::test]
    async fn lists_objects_and_prefixes_across_pages() {
        let (client, requests) = listing_client(vec![
            "<ListBucketResult><IsTruncated>true</IsTruncated><CommonPrefixes><Prefix>photos/sub/</Prefix></CommonPrefixes><NextContinuationToken>next</NextContinuationToken></ListBucketResult>",
            "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>photos/a.jpg</Key><Size>42</Size></Contents></ListBucketResult>",
        ]).await;
        let connection = S3Connection {
            client,
            bucket: Some("example".into()),
            redactions: Vec::new(),
        };
        let mut output = crate::core::OutputStore::new(9002);
        assert_eq!(connection.list("photos/", &mut output).await.unwrap(), 2);
        let lines = output.read_all();
        assert_eq!(lines[0], "DIR  photos/sub/");
        assert!(lines[1].contains("42  photos/a.jpg"));
        let requests = requests.await.unwrap();
        assert!(
            requests[0].starts_with("GET /example/") || requests[0].starts_with("GET /example?"),
            "{}",
            requests[0]
        );
        assert!(requests[1].contains("continuation-token=next"));
        std::fs::remove_file(output.path()).unwrap();
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
        };
        let connection = S3
            .connect(ConnectOptions {
                target: "test".into(),
                bookmark: Some(bookmark),
                settings,
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
