/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! S3 has no native rename: conditional copy, verify, then conditional delete.
use super::{operation_error, S3Connection};
use crate::{backend::validate_rename_paths, error::NonRetryableError};
use anyhow::Result;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
const COPY_PATH: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
pub(super) fn strong_tag(tag: &str) -> bool {
    tag.len() >= 2
        && tag.starts_with('"')
        && tag.ends_with('"')
        && !tag[1..tag.len() - 1].contains('"')
        && !tag.chars().any(char::is_control)
}
fn permanent(message: &str) -> anyhow::Error {
    NonRetryableError(message.into()).into()
}
impl S3Connection {
    pub(super) async fn move_object(
        &self,
        source: &str,
        destination: &str,
        overwrite: bool,
    ) -> Result<()> {
        validate_rename_paths(source, destination)?;
        let from = self.location.resolve(source);
        let to = self.location.resolve(destination);
        let (Some(bucket), Some(target_bucket)) = (from.bucket.as_deref(), to.bucket.as_deref())
        else {
            return Err(permanent(
                "S3 mv requires object paths inside existing buckets",
            ));
        };
        let key = from.prefix.trim_end_matches('/');
        let target_key = to.prefix.trim_end_matches('/');
        if key.is_empty() || target_key.is_empty() {
            return Err(permanent("S3 mv cannot rename buckets or prefixes"));
        }
        let before = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| operation_error("HeadObject", &error, &self.redactions))?;
        if from == to {
            return Ok(());
        }
        let size = before
            .content_length()
            .filter(|size| *size >= 0)
            .ok_or_else(|| permanent("S3 source has no valid length"))?;
        if size as u64 > super::upload::MAX_OBJECT_SIZE {
            return Err(permanent("S3 mv supports objects up to 5 TiB"));
        }
        let tag = before
            .e_tag()
            .filter(|tag| strong_tag(tag))
            .ok_or_else(|| permanent("S3 mv requires a strong quoted source ETag"))?;
        let mut copy_source =
            utf8_percent_encode(&format!("{bucket}/{key}"), COPY_PATH).to_string();
        if let Some(version) = before
            .version_id()
            .filter(|id| !id.is_empty() && *id != "null")
        {
            copy_source.push_str("?versionId=");
            copy_source.push_str(&utf8_percent_encode(version, NON_ALPHANUMERIC).to_string());
        }
        let copied_tag = if size as u64 > super::upload::SINGLE_LIMIT {
            self.multipart_copy(
                bucket,
                key,
                target_bucket,
                target_key,
                &copy_source,
                tag,
                &before,
                size as u64,
                overwrite,
            )
            .await?
        } else {
            let mut copy = self
                .client
                .copy_object()
                .bucket(target_bucket)
                .key(target_key)
                .copy_source(copy_source)
                .copy_source_if_match(tag)
                .set_storage_class(before.storage_class().cloned())
                .set_website_redirect_location(
                    before.website_redirect_location().map(str::to_owned),
                );
            if !overwrite {
                copy = copy.if_none_match("*");
            }
            let copied = copy.send().await.map_err(|error| {
                let cause = operation_error("CopyObject", &error, &self.redactions);
                let message = format!(
                    "S3 copy failed or its outcome is unknown; source was not deleted: {cause}"
                );
                cause.context(message)
            })?;
            // A 200 HTTP status is not enough: CopyObject may carry an embedded
            // failure, and a malformed success must never authorize deletion.
            let copied_tag = copied
                .copy_object_result()
                .and_then(|result| result.e_tag())
                .filter(|tag| strong_tag(tag))
                .ok_or_else(|| {
                    permanent("S3 copy has no ETag; source was not deleted; inspect destination")
                })?
                .to_owned();
            copied_tag
        };
        let result: Result<()> = async {
            let destination = self
                .client
                .head_object()
                .bucket(target_bucket)
                .key(target_key)
                .send()
                .await
                .map_err(|error| operation_error("HeadObject", &error, &self.redactions))?;
            if destination.content_length() != Some(size)
                || destination.e_tag() != Some(copied_tag.as_str())
            {
                return Err(permanent(
                    "S3 copied destination changed or failed verification",
                ));
            }
            let current = self
                .client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|error| operation_error("HeadObject", &error, &self.redactions))?;
            if current.content_length() != Some(size)
                || current.e_tag() != Some(tag)
                || current.version_id() != before.version_id()
                || current.last_modified() != before.last_modified()
            {
                return Err(permanent("S3 source changed during copy"));
            }
            self.client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .if_match(tag)
                .send()
                .await
                .map_err(|error| operation_error("DeleteObject", &error, &self.redactions))?;
            Ok(())
        }
        .await;
        result.map_err(|error| {
            let message = format!("S3 copy completed but move did not complete; both paths may exist (delete outcome may be unknown); inspect before repeating: {error}");
            error.context(message)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Connection, s3::connection_tests::http_client};
    fn head(size: i64, tag: Option<&str>, version: Option<&str>) -> (String, Vec<u8>) {
        (
            format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {size}\r\n{}{}connection: close\r\n\r\n",
                tag.map(|tag| format!("etag: {tag}\r\n"))
                    .unwrap_or_default(),
                version
                    .map(|version| format!("x-amz-version-id: {version}\r\n"))
                    .unwrap_or_default()
            ),
            Vec::new(),
        )
    }
    fn response(status: u16, body: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 {status} Response\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",body.len()),body.as_bytes().into())
    }
    fn copied() -> (String, Vec<u8>) {
        response(
            200,
            "<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>",
        )
    }
    fn original() -> (String, Vec<u8>) {
        head(3, Some("\"src\""), None)
    }
    fn destination() -> (String, Vec<u8>) {
        head(3, Some("\"copy\""), None)
    }
    fn assert_not_deleted(requests: &[String]) {
        assert!(
            requests
                .iter()
                .all(|request| !request.starts_with("DELETE ")),
            "{requests:?}"
        );
    }
    #[tokio::test]
    async fn copy_verify_and_delete_use_exact_encoded_paths_and_conditionals() {
        for overwrite in [false, true] {
            let (connection, requests) = http_client(vec![
                original(),
                copied(),
                destination(),
                original(),
                response(204, ""),
            ])
            .await;
            connection
                .rename_file("dir/日本語 +%?#*.txt", "/second/new file", overwrite)
                .await
                .unwrap();
            assert_eq!(connection.pwd().unwrap(), "/example/");
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 5);
            let copy = requests[1].to_ascii_lowercase();
            assert!(copy.starts_with("put /second/new%20file"));
            assert!(copy.contains("x-amz-copy-source: example/dir/%e6%97%a5%e6%9c%ac%e8%aa%9e%20%2b%25%3f%23%2a.txt\r\n"),"{copy}");
            assert!(copy.contains("x-amz-copy-source-if-match: \"src\"\r\n"));
            assert_eq!(copy.contains("if-none-match: *\r\n"), !overwrite);
            let delete = requests[4].to_ascii_lowercase();
            assert!(delete.starts_with("delete /example/dir/"));
            assert!(delete.contains("if-match: \"src\"\r\n"));
            assert!(!delete.contains("versionid="));
        }
    }
    #[tokio::test]
    async fn failed_embedded_or_ambiguous_copy_never_deletes_source_or_retries_unconditionally() {
        for result in [
            response(412, "<Error><Code>PreconditionFailed</Code></Error>"),
            response(501, "<Error><Code>NotImplemented</Code></Error>"),
            response(503, "<Error><Code>SlowDown</Code></Error>"),
            response(200, "<Error><Code>InternalError</Code></Error>"),
            response(200, "<CopyObjectResult/>"),
            (String::new(), Vec::new()),
        ] {
            let (connection, requests) = http_client(vec![original(), result]).await;
            let error = connection.rename_file("a", "b", false).await.unwrap_err();
            assert!(
                error.to_string().contains("source was not deleted"),
                "{error}"
            );
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 2);
            assert_not_deleted(&requests);
            assert!(requests[1]
                .to_ascii_lowercase()
                .contains("if-none-match: *\r\n"));
        }
    }
    #[tokio::test]
    async fn verification_and_source_changes_leave_both_objects_without_rollback() {
        for (after_dest, after_src) in [
            (head(4, Some("\"copy\""), None), None),
            (head(3, Some("\"other\""), None), None),
            (response(404, ""), None),
            (destination(), Some(head(3, Some("\"changed\""), None))),
            (destination(), Some(head(4, Some("\"src\""), None))),
            (
                destination(),
                Some(head(3, Some("\"src\""), Some("new-version"))),
            ),
            (destination(), Some(response(404, ""))),
        ] {
            let mut responses = vec![original(), copied(), after_dest];
            if let Some(after_src) = after_src {
                responses.push(after_src);
            }
            let (connection, requests) = http_client(responses).await;
            let error = connection.rename_file("a", "b", false).await.unwrap_err();
            assert!(error
                .to_string()
                .contains("copy completed but move did not complete"));
            assert_not_deleted(&requests.await.unwrap());
        }
    }
    #[tokio::test]
    async fn delete_failure_preserves_copy_reports_partial_outcome_and_never_replays() {
        for result in [
            response(412, "<Error><Code>PreconditionFailed</Code></Error>"),
            response(403, "<Error><Code>AccessDenied</Code></Error>"),
            (String::new(), Vec::new()),
        ] {
            let (connection, requests) = http_client(vec![
                original(),
                copied(),
                destination(),
                original(),
                result,
            ])
            .await;
            let error = connection.rename_file("a", "b", true).await.unwrap_err();
            assert!(error.to_string().contains("delete outcome may be unknown"));
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 5);
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.starts_with("DELETE "))
                    .count(),
                1
            );
            assert!(requests[4].starts_with("DELETE /example/a"));
        }
    }
    #[tokio::test]
    async fn versioned_copies_pin_the_revision_but_delete_creates_a_marker_not_an_old_version_reveal(
    ) {
        let source = || head(3, Some("\"src\""), Some("v+/?"));
        let (connection, requests) = http_client(vec![
            source(),
            copied(),
            destination(),
            source(),
            response(204, ""),
        ])
        .await;
        connection.rename_file("a", "b", false).await.unwrap();
        let requests = requests.await.unwrap();
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("x-amz-copy-source: example/a?versionid=v%2b%2f%3f\r\n"));
        assert!(!requests[4].to_ascii_lowercase().contains("versionid="));
    }
    #[tokio::test]
    async fn exact_object_validation_limits_and_same_path_never_mutate_or_list_prefixes() {
        for (a, b) in [
            ("/", "b"),
            ("/example", "b"),
            ("a", "/second/"),
            ("a/", "b"),
            ("a", "."),
            ("", "b"),
        ] {
            let (connection, requests) = http_client(vec![]).await;
            assert!(connection
                .rename_file(a, b, false)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            assert!(requests.await.unwrap().is_empty());
        }
        for meta in [
            head(3, None, None),
            head(3, Some("*"), None),
            head(3, Some("unquoted"), None),
            head(3, Some("W/\"weak\""), None),
            head(5_i64 * 1024 * 1024 * 1024 * 1024 + 1, Some("\"src\""), None),
        ] {
            let (connection, requests) = http_client(vec![meta]).await;
            assert!(connection
                .rename_file("a", "b", false)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 1);
            assert_not_deleted(&requests);
        }
        let (connection, requests) = http_client(vec![original()]).await;
        connection.rename_file("a", "./a", false).await.unwrap();
        assert_eq!(requests.await.unwrap().len(), 1);
    }
    #[tokio::test]
    async fn provider_diagnostics_remain_redacted_on_partial_and_initial_failures() {
        for partial in [false, true] {
            let error_response = response(403,"<Error><Code>AccessDenied</Code><Message>secret-value&#10;other details</Message></Error>");
            let responses = if partial {
                vec![
                    original(),
                    copied(),
                    destination(),
                    original(),
                    error_response,
                ]
            } else {
                vec![original(), error_response]
            };
            let (mut connection, requests) = http_client(responses).await;
            connection.redactions.push("secret-value".into());
            let error = connection.rename_file("a", "b", false).await.unwrap_err();
            assert!(error.is::<NonRetryableError>());
            assert!(error.to_string().contains("[redacted]"));
            assert!(!format!("{error:#}").contains("secret-value"));
            requests.await.unwrap();
        }
    }
    #[tokio::test]
    async fn copy_preserves_source_storage_class_and_website_redirect_metadata() {
        let mut metadata = original();
        metadata.0 = metadata.0.replace("connection: close", "x-amz-storage-class: STANDARD_IA\r\nx-amz-website-redirect-location: /redirect\r\nconnection: close");
        let (connection, requests) = http_client(vec![
            metadata.clone(),
            copied(),
            destination(),
            metadata,
            response(204, ""),
        ])
        .await;
        connection.rename_file("a", "b", false).await.unwrap();
        let requests = requests.await.unwrap();
        let headers = requests[1].to_ascii_lowercase();
        assert!(headers.contains("x-amz-storage-class: standard_ia\r\n"));
        assert!(headers.contains("x-amz-website-redirect-location: /redirect\r\n"));
    }
    #[tokio::test]
    async fn cancellation_before_copy_reply_or_after_copy_never_schedules_a_delete() {
        use hyper::{
            service::{make_service_fn, service_fn},
            Body, Response, Server,
        };
        use std::{
            convert::Infallible,
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            time::Duration,
        };
        for stall_copy in [true, false] {
            let copied_count = Arc::new(AtomicUsize::new(0));
            let deleted_count = Arc::new(AtomicUsize::new(0));
            let reached = Arc::new(tokio::sync::Notify::new());
            let (base, old_requests) = http_client(vec![]).await;
            old_requests.await.unwrap();
            let server = Server::bind(&"127.0.0.1:0".parse().unwrap());
            let address = server.local_addr();
            let (copied_requests, deleted_requests, arrived) =
                (copied_count.clone(), deleted_count.clone(), reached.clone());
            let server = server.serve(make_service_fn(move |_| {
                let (copied,deleted,reached) = (copied_requests.clone(),deleted_requests.clone(),arrived.clone());
                async move { Ok::<_,Infallible>(service_fn(move |request: hyper::Request<Body>| {
                    let (copied,deleted,reached) = (copied.clone(),deleted.clone(),reached.clone());
                    async move {
                        let method = request.method().as_str(); let path = request.uri().path();
                        if method == "PUT" {
                            copied.fetch_add(1,Ordering::SeqCst);
                            if stall_copy { reached.notify_one(); std::future::pending::<()>().await; }
                            return Ok::<_,Infallible>(Response::new(Body::from("<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>")));
                        }
                        if method == "HEAD" && path == "/example/b" {
                            reached.notify_one(); std::future::pending::<()>().await;
                        }
                        if method == "DELETE" { deleted.fetch_add(1,Ordering::SeqCst); }
                        Ok(Response::builder().status(200).header("content-length","3").header("etag","\"src\"").body(Body::empty()).unwrap())
                    }
                })) }
            }));
            let task = tokio::spawn(async move {
                let _ = server.await;
            });
            let client = aws_sdk_s3::Client::from_conf(
                base.client
                    .config()
                    .to_builder()
                    .endpoint_url(format!("http://{address}"))
                    .build(),
            );
            let connection = S3Connection::new(client, Some("example".into()), Vec::new());
            let mut moving = Box::pin(connection.rename_file("a", "b", false));
            tokio::select! {
                result = &mut moving => panic!("move unexpectedly completed: {result:?}"),
                ready = tokio::time::timeout(Duration::from_secs(3),reached.notified()) => { ready.expect("move did not reach the paused phase"); },
            }
            drop(moving);
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(copied_count.load(Ordering::SeqCst), 1);
            assert_eq!(deleted_count.load(Ordering::SeqCst), 0);
            task.abort();
        }
    }
}

#[cfg(test)]
mod remove_tests {
    use crate::{backend::Connection, error::NonRetryableError, s3::connection_tests::http_client};
    fn response(status: u16, headers: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 {status} Response\r\n{headers}content-length: 0\r\nconnection: close\r\n\r\n"),Vec::new())
    }
    #[tokio::test]
    async fn remove_empty_s3_directory_only_deletes_its_marker_conditionally() {
        fn listing(body: &str) -> (String, Vec<u8>) {
            (format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()), body.as_bytes().to_vec())
        }
        let marker = "<ListBucketResult><Contents><Key>dir/</Key><ETag>&quot;v1&quot;</ETag></Contents></ListBucketResult>";
        let (connection, requests) = http_client(vec![listing(marker), response(204, "")]).await;
        connection.remove_empty_directory("/example/dir").await.unwrap();
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("DELETE /example/dir/"));
        assert!(requests[1].to_ascii_lowercase().contains("if-match: \"v1\"\r\n"));
        let nonempty = "<ListBucketResult><Contents><Key>dir/file</Key></Contents></ListBucketResult>";
        let (connection, requests) = http_client(vec![listing(nonempty)]).await;
        assert!(connection.remove_empty_directory("/example/dir").await.is_err());
        assert_eq!(requests.await.unwrap().len(), 1);
        let (connection, requests) = http_client(vec![]).await;
        assert!(connection.remove_empty_directory("/example").await.is_err());
        assert!(requests.await.unwrap().is_empty());
    }
    #[tokio::test]
    async fn remove_requires_etag_and_deletes_only_exact_object_conditionally() {
        let (connection, requests) =
            http_client(vec![response(200, "etag: \"v1\"\r\n"), response(204, "")]).await;
        connection.remove_file("dir/雪 %.txt").await.unwrap();
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("HEAD /example/dir/"));
        assert!(requests[1].starts_with("DELETE /example/dir/"));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("if-match: \"v1\"\r\n"));
        assert!(!requests[1].contains("versionId"));
        for header in ["", "etag: *\r\n", "etag: W/\"weak\"\r\n"] {
            let (connection, requests) = http_client(vec![response(200, header)]).await;
            assert!(connection
                .remove_file("file")
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            assert_eq!(requests.await.unwrap().len(), 1);
        }
        for path in ["/", "/example", "dir/", "."] {
            let (connection, requests) = http_client(vec![]).await;
            assert!(connection
                .remove_file(path)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            assert!(requests.await.unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn removal_does_not_retry_changed_or_ambiguous_deletes() {
        for response in [response(412, ""), (String::new(), Vec::new())] {
            let (connection, requests) =
                http_client(vec![self::response(200, "etag: \"v1\"\r\n"), response]).await;
            assert!(connection.remove_file("file").await.is_err());
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.starts_with("DELETE "))
                    .count(),
                1
            );
        }
    }
}
