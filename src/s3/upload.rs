//! Stream single-request object uploads without buffering the file in memory.
use super::{operation_error, S3Connection};
use crate::{backend::validate_file_path, error::NonRetryableError};
use anyhow::Result;
use aws_sdk_s3::primitives::{ByteStream, Length};
use std::{
    fs::OpenOptions,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

impl S3Connection {
    pub(super) async fn upload_object(
        &self,
        local: &Path,
        path: &str,
        overwrite: bool,
    ) -> Result<()> {
        validate_file_path(path).map_err(|_| {
            NonRetryableError(
                "put requires an exact S3 object filename (at most 64 KiB), not a directory".into(),
            )
        })?;
        let target = self.location.resolve(path);
        let bucket = target
            .bucket
            .as_deref()
            .ok_or_else(|| NonRetryableError("select an S3 bucket before uploading".into()))?;
        let key = target.prefix.trim_end_matches('/');
        if key.is_empty() {
            return Err(NonRetryableError(
                "put expects an S3 object filename, not a bucket".into(),
            )
            .into());
        }
        // Keep one descriptor so a substituted symlink/FIFO cannot be followed.
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(local)
            .map_err(|error| {
                NonRetryableError(format!("cannot open local upload file: {error}"))
            })?;
        let before = file.metadata().map_err(|error| {
            NonRetryableError(format!("cannot stat local upload file: {error}"))
        })?;
        if !before.is_file() {
            return Err(NonRetryableError("upload requires a regular local file".into()).into());
        }
        if before.len() > 5 * 1024 * 1024 * 1024 {
            return Err(NonRetryableError(
                "S3 uploads support files up to 5 GiB; larger files require multipart upload"
                    .into(),
            )
            .into());
        }
        let check = file.try_clone().map_err(|error| {
            NonRetryableError(format!("cannot retain local upload handle: {error}"))
        })?;
        let body = ByteStream::read_from()
            .file(tokio::fs::File::from_std(file))
            .length(Length::Exact(before.len()))
            .build()
            .await
            .map_err(|error| {
                NonRetryableError(format!("cannot stream local upload file: {error}"))
            })?;
        let mut request = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_length(before.len() as i64)
            .body(body);
        // An atomic conditional write, never a racy HEAD-then-PUT check.
        if !overwrite {
            request = request.if_none_match("*");
        }
        // Upload duration scales with file size; retain a stalled-read timeout
        // rather than the connection-wide 30-second total operation deadline.
        let timeout = aws_config::timeout::TimeoutConfig::builder()
            .disable_operation_timeout()
            .read_timeout(std::time::Duration::from_secs(30))
            .build();
        request
            .customize()
            .config_override(aws_sdk_s3::config::Builder::new().timeout_config(timeout))
            .send()
            .await
            .map_err(|error| operation_error("PutObject", &error, &self.redactions))?;
        let after = check
            .metadata()
            .map_err(|error| NonRetryableError(format!("cannot stat uploaded file: {error}")))?;
        if before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err(NonRetryableError(
                "local file changed during upload; remote object was retained".into(),
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Connection;
    use hyper::{service::service_fn, Body, Request, Response};
    use std::{convert::Infallible, io::Write};

    async fn endpoint(
        status: u16,
    ) -> (
        S3Connection,
        tokio::sync::oneshot::Receiver<(hyper::HeaderMap, Vec<u8>, String)>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let send = std::sync::Arc::new(std::sync::Mutex::new(Some(send)));
            hyper::server::conn::Http::new()
                .serve_connection(
                    socket,
                    service_fn(move |request: Request<Body>| {
                        let send = send.clone();
                        async move {
                            let (parts, body) = request.into_parts();
                            let body = hyper::body::to_bytes(body).await.unwrap();
                            send.lock()
                                .unwrap()
                                .take()
                                .unwrap()
                                .send((
                                    parts.headers,
                                    body.to_vec(),
                                    format!("{} {}", parts.method, parts.uri),
                                ))
                                .unwrap();
                            let body = if status == 200 {
                                ""
                            } else {
                                "<Error><Code>UploadError</Code></Error>"
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("connection", "close")
                                    .body(Body::from(body))
                                    .unwrap(),
                            )
                        }
                    }),
                )
                .await
                .unwrap();
        });
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
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

    #[tokio::test]
    async fn uploads_streamed_files_and_empty_objects_with_atomic_clobber_control() {
        for overwrite in [false, true] {
            for payload in [b"".as_slice(), b"s3 upload payload".as_slice()] {
                let (mut connection, received) = endpoint(200).await;
                connection.location = connection.location.resolve("photos");
                let mut file = tempfile::NamedTempFile::new().unwrap();
                file.write_all(payload).unwrap();
                connection
                    .upload(file.path(), "../file name.txt", overwrite)
                    .await
                    .unwrap();
                assert_eq!(connection.pwd().unwrap(), "/example/photos/");
                let (headers, body, target) = received.await.unwrap();
                assert!(
                    target.starts_with("PUT /example/file%20name.txt"),
                    "{target}"
                );
                assert_eq!(
                    headers.get("if-none-match").map(|v| v.to_str().unwrap()),
                    if overwrite { None } else { Some("*") }
                );
                // The SDK may wrap the payload in aws-chunked checksum framing.
                if !payload.is_empty() {
                    assert!(body.windows(payload.len()).any(|window| window == payload));
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_uploads_classify_permanent_and_transient_errors_without_replay() {
        for (status, permanent) in [(412, true), (403, true), (409, false), (503, false)] {
            let (connection, received) = endpoint(status).await;
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(b"data").unwrap();
            let error = connection
                .upload(file.path(), "a.txt", false)
                .await
                .unwrap_err();
            assert_eq!(error.is::<NonRetryableError>(), permanent, "{error}");
            assert!(
                error.to_string().contains(&format!("HTTP {status}")),
                "{error}"
            );
            let (headers, _, _) = received.await.unwrap();
            assert_eq!(headers["if-none-match"], "*");
        }
    }

    #[tokio::test]
    async fn invalid_sources_and_targets_fail_before_network_io() {
        let (mut connection, _) = endpoint(200).await;
        let file = tempfile::NamedTempFile::new().unwrap();
        for path in ["", "/", "/example", "directory/", ".."] {
            assert!(connection
                .upload(file.path(), path, false)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
        }
        let directory = tempfile::tempdir().unwrap();
        assert!(connection
            .upload(directory.path(), "a", false)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(file.path(), &link).unwrap();
        assert!(connection
            .upload(&link, "a", false)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        file.as_file().set_len(5 * 1024 * 1024 * 1024 + 1).unwrap();
        assert!(connection
            .upload(file.path(), "a", false)
            .await
            .unwrap_err()
            .to_string()
            .contains("multipart"));
        connection.location = super::super::S3Location::default();
        assert!(connection
            .upload(file.path(), "bucket", false)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
    }

    #[tokio::test]
    async fn put_and_mirror_upload_commands_upload_s3_files_and_directory_markers() {
        use crate::{core::JobState, daemon::Daemon, s3::connection_tests::http_client};
        fn response(body: &str) -> (String, Vec<u8>) {
            (
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                ),
                body.as_bytes().to_vec(),
            )
        }
        let (connection, requests) = http_client(vec![
            response(""), // put standalone.txt
            response(""), // HeadBucket for mirror root
            response("<ListBucketResult></ListBucketResult>"),
            response(""), // create tree/ marker
            response(""), // tree/nested.txt
        ])
        .await;
        let local = tempfile::tempdir().unwrap();
        std::fs::write(local.path().join("standalone.txt"), b"standalone contents").unwrap();
        std::fs::create_dir(local.path().join("tree")).unwrap();
        std::fs::write(local.path().join("tree/nested.txt"), b"nested contents").unwrap();
        let mut app = Daemon::bootstrap().unwrap();
        app.local_directory = local.path().to_owned();
        *app.connection.lock().await = Some(Box::new(connection));
        for command in ["put standalone.txt", "mirror-upload tree"] {
            let reply = app.command(command);
            let id = reply[0]
                .strip_prefix("JOB ")
                .unwrap()
                .parse::<u64>()
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while app.has_running_jobs() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                app.jobs.jobs[&id].state,
                JobState::Completed,
                "{:?}",
                app.jobs.jobs[&id].output.read_all()
            );
        }
        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("PUT /example/standalone.txt"));
        assert!(requests[0].contains("standalone contents"));
        assert!(requests[3].starts_with("PUT /example/tree/"));
        assert!(requests[4].starts_with("PUT /example/tree/nested.txt"));
        assert!(requests[4].contains("nested contents"));
        for request in requests
            .iter()
            .filter(|request| request.starts_with("PUT "))
        {
            assert!(request.to_ascii_lowercase().contains("if-none-match: *"));
        }
        assert_eq!(
            app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
            "/example/"
        );
    }
}
