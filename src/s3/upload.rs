//! Stream S3 uploads from disk, using multipart uploads for large files.
use super::{operation_error, S3Connection};
use crate::{backend::validate_file_path, error::NonRetryableError};
use anyhow::Result;
use aws_sdk_s3::primitives::{ByteStream, Length};
use std::{
    fs::OpenOptions,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

const SINGLE_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const MIN_PART: u64 = 16 * 1024 * 1024;

fn part_size(size: u64) -> u64 {
    MIN_PART.max(size.div_ceil(10_000).div_ceil(1024 * 1024) * 1024 * 1024)
}

fn unchanged(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

// Cancellation drops the future, so schedule a best-effort abort independently.
// An interrupted CreateMultipartUpload with no received ID cannot be cleaned up.
struct MultipartGuard {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    id: Option<String>,
}
impl Drop for MultipartGuard {
    fn drop(&mut self) {
        if let (Some(id), Ok(handle)) = (self.id.take(), tokio::runtime::Handle::try_current()) {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            handle.spawn(async move {
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(id)
                    .send()
                    .await;
            });
        }
    }
}

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
        if before.len() > MAX_OBJECT_SIZE {
            return Err(NonRetryableError("S3 uploads support files up to 5 TiB".into()).into());
        }
        if before.len() > SINGLE_LIMIT {
            let size = part_size(before.len());
            return self
                .multipart_upload(bucket, key, file, before, overwrite, size)
                .await;
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
        if !unchanged(&before, &after) {
            return Err(NonRetryableError(
                "local file changed during upload; remote object was retained".into(),
            )
            .into());
        }
        Ok(())
    }

    async fn multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        file: std::fs::File,
        before: std::fs::Metadata,
        overwrite: bool,
        part_size: u64,
    ) -> Result<()> {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
        let created = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| operation_error("CreateMultipartUpload", &e, &self.redactions))?;
        let id = created.upload_id().filter(|id| !id.is_empty())
            .ok_or_else(|| NonRetryableError("S3 multipart initiation returned no upload ID; an incomplete upload may remain".into()))?.to_owned();
        let mut guard = MultipartGuard {
            client: self.client.clone(),
            bucket: bucket.into(),
            key: key.into(),
            id: Some(id.clone()),
        };
        let mut parts = Vec::new();
        let mut offset = 0;
        while offset < before.len() {
            let length = part_size.min(before.len() - offset);
            // Clones share the offset; parts are deliberately sent sequentially.
            let mut source = file
                .try_clone()
                .map_err(|e| NonRetryableError(format!("cannot retain upload file: {e}")))?;
            std::io::Seek::seek(&mut source, std::io::SeekFrom::Start(offset))
                .map_err(|e| NonRetryableError(format!("cannot seek upload file: {e}")))?;
            let body = ByteStream::read_from()
                .file(tokio::fs::File::from_std(source))
                .length(Length::Exact(length))
                .build()
                .await
                .map_err(|e| NonRetryableError(format!("cannot stream upload part: {e}")))?;
            let number = parts.len() as i32 + 1;
            let timeout = aws_config::timeout::TimeoutConfig::builder()
                .disable_operation_timeout()
                .read_timeout(std::time::Duration::from_secs(30))
                .build();
            let response = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&id)
                .part_number(number)
                .content_length(length as i64)
                .body(body)
                .customize()
                .config_override(aws_sdk_s3::config::Builder::new().timeout_config(timeout))
                .send()
                .await
                .map_err(|e| operation_error("UploadPart", &e, &self.redactions))?;
            let tag = response
                .e_tag()
                .filter(|tag| !tag.is_empty())
                .ok_or_else(|| NonRetryableError("S3 upload part returned no ETag".into()))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(tag)
                    .build(),
            );
            offset += length;
        }
        let after = file
            .metadata()
            .map_err(|e| NonRetryableError(format!("cannot stat upload file: {e}")))?;
        if !unchanged(&before, &after) {
            return Err(NonRetryableError(
                "local file changed during upload; multipart upload was not completed".into(),
            )
            .into());
        }
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            );
        if !overwrite {
            complete = complete.if_none_match("*");
        }
        // A lost completion reply is ambiguous. Never automatically retry it: the
        // object may already exist, especially dangerous with clobber enabled.
        let timeout = aws_config::timeout::TimeoutConfig::builder()
            .disable_operation_timeout()
            .read_timeout(std::time::Duration::from_secs(120))
            .build();
        complete.customize()
            .config_override(aws_sdk_s3::config::Builder::new().timeout_config(timeout))
            .send().await.map_err(|e| {
            let cause = operation_error("CompleteMultipartUpload", &e, &self.redactions);
            NonRetryableError(format!("multipart completion failed or its outcome is unknown; inspect the destination before retrying: {cause}"))
        })?;
        guard.id = None;
        let after = file
            .metadata()
            .map_err(|e| NonRetryableError(format!("cannot stat uploaded file: {e}")))?;
        if !unchanged(&before, &after) {
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
        file.as_file().set_len(MAX_OBJECT_SIZE + 1).unwrap();
        assert!(connection
            .upload(file.path(), "a", false)
            .await
            .unwrap_err()
            .to_string()
            .contains("5 TiB"));
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

    fn response(status: u16, headers: &str, body: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 {status} Response\r\ncontent-type: application/xml\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n", body.len()), body.as_bytes().to_vec())
    }

    #[test]
    fn multipart_sizes_fit_s3_limits() {
        for size in [SINGLE_LIMIT + 1, MAX_OBJECT_SIZE] {
            let part = part_size(size);
            assert!(part >= 5 * 1024 * 1024);
            assert!(part <= SINGLE_LIMIT);
            assert!(size.div_ceil(part) <= 10_000);
        }
    }

    #[tokio::test]
    async fn multipart_streams_parts_and_completes_conditionally() {
        use crate::s3::connection_tests::http_client;
        for overwrite in [false, true] {
            let (connection, requests) = http_client(vec![
                response(200, "", "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"),
                response(200, "etag: \"part1\"\r\n", ""),
                response(200, "etag: \"part2\"\r\n", ""),
                response(200, "", "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>"),
            ]).await;
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"abcdefgh").unwrap();
            let before = file.metadata().unwrap();
            connection
                .multipart_upload("example", "big file", file, before, overwrite, 5)
                .await
                .unwrap();
            let requests = requests.await.unwrap();
            assert!(requests[0].starts_with("POST /example/big%20file?uploads"));
            assert!(requests[1].contains("partNumber=1"));
            assert!(requests[1].contains("abcde"));
            assert!(requests[2].contains("partNumber=2"));
            assert!(requests[2].contains("fgh"));
            assert!(requests[3].contains("uploadId=upload-1"));
            assert!(requests[3].contains("part1") && requests[3].contains("part2"));
            assert_eq!(
                requests[3]
                    .to_ascii_lowercase()
                    .contains("if-none-match: *"),
                !overwrite
            );
        }
    }

    #[tokio::test]
    async fn multipart_failure_aborts_and_completion_conflict_is_not_retried() {
        use crate::s3::connection_tests::http_client;
        for during_complete in [false, true] {
            let mut responses = vec![response(200, "", "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>")];
            if during_complete {
                responses.push(response(200, "etag: \"part1\"\r\n", ""));
            }
            responses.push(response(
                if during_complete { 412 } else { 403 },
                "",
                "<Error><Code>AccessDenied</Code></Error>",
            ));
            responses.push(response(204, "", ""));
            let (connection, requests) = http_client(responses).await;
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"data").unwrap();
            let before = file.metadata().unwrap();
            let error = connection
                .multipart_upload("example", "key", file, before, false, 5)
                .await
                .unwrap_err();
            assert!(error.is::<NonRetryableError>());
            if during_complete {
                assert!(error.to_string().contains("inspect the destination"));
            }
            let requests = tokio::time::timeout(std::time::Duration::from_secs(5), requests)
                .await
                .unwrap()
                .unwrap();
            let abort = requests.last().unwrap();
            assert!(abort.starts_with("DELETE /example/key?"), "{abort}");
            assert!(abort.contains("uploadId=upload-1"), "{abort}");
        }
    }

    #[tokio::test]
    async fn cancelling_part_upload_schedules_abort() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (part_tx, part_rx) = tokio::sync::oneshot::channel();
        let (abort_tx, abort_rx) = tokio::sync::oneshot::channel();
        let part_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(part_tx)));
        let abort_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(abort_tx)));
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let part = part_tx.clone();
                let abort = abort_tx.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Body>| {
                        let part = part.clone();
                        let abort = abort.clone();
                        async move {
                            let method = request.method().clone();
                            let _ = hyper::body::to_bytes(request.into_body()).await;
                            if method == hyper::Method::PUT {
                                if let Some(tx) = part.lock().unwrap().take() {
                                    let _ = tx.send(());
                                }
                                std::future::pending::<()>().await;
                            }
                            let body = if method == hyper::Method::POST {
                                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                            } else {
                                if let Some(tx) = abort.lock().unwrap().take() {
                                    let _ = tx.send(());
                                }
                                ""
                            };
                            Ok::<_, Infallible>(Response::new(Body::from(body)))
                        }
                    });
                    let _ = hyper::server::conn::Http::new()
                        .serve_connection(socket, service)
                        .await;
                });
            }
        });
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .endpoint_url(format!("http://{address}"))
            .force_path_style(true)
            .build();
        let connection = S3Connection::new(
            aws_sdk_s3::Client::from_conf(config),
            Some("example".into()),
            Vec::new(),
        );
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"data").unwrap();
        let before = file.metadata().unwrap();
        let task = tokio::spawn(async move {
            connection
                .multipart_upload("example", "key", file, before, false, 5)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), part_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(5), abort_rx)
            .await
            .unwrap()
            .unwrap();
        server.abort();
    }
}
