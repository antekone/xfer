//! Directory creation for S3's virtual namespace, without creating buckets or
//! replacing any existing object (including a nonempty trailing-slash object).
use super::{operation_error, S3Connection};
use crate::error::NonRetryableError;
use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;

impl S3Connection {
    pub(super) async fn mkdir(&self, path: &str, parents: bool) -> Result<()> {
        let target = self.location.resolve(path);
        let bucket = target.bucket.as_deref().ok_or_else(|| {
            NonRetryableError("mkdir requires a directory inside an existing S3 bucket".into())
        })?;
        // A bucket is a boundary, never a missing parent that -p may create.
        self.client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| operation_error("HeadBucket", &error, &self.redactions))?;
        if target.prefix.is_empty() {
            return if parents {
                Ok(())
            } else {
                Err(NonRetryableError(format!("S3 bucket already exists: {bucket:?}")).into())
            };
        }
        if parents {
            let mut prefix = String::new();
            for component in target.prefix.split_inclusive('/') {
                prefix.push_str(component);
                if !self.directory_exists(bucket, &prefix).await? {
                    self.create_marker(bucket, &prefix, true).await?;
                }
            }
        } else {
            if self.directory_exists(bucket, &target.prefix).await? {
                return Err(already_exists(&target.prefix));
            }
            if let Some((parent, _)) = target.prefix.trim_end_matches('/').rsplit_once('/') {
                let parent = format!("{parent}/");
                if !self.directory_exists(bucket, &parent).await? {
                    return Err(
                        NonRetryableError(format!("no such parent directory: {parent:?}")).into(),
                    );
                }
            }
            self.create_marker(bucket, &target.prefix, false).await?;
        }
        Ok(())
    }

    async fn directory_exists(&self, bucket: &str, prefix: &str) -> Result<bool> {
        // Both explicit markers and ordinary descendant objects establish a
        // directory. No HeadObject/GetObject permission or object download needed.
        let result = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1)
            .send()
            .await
            .map_err(|error| operation_error("ListObjectsV2", &error, &self.redactions))?;
        Ok(!result.contents().is_empty() || !result.common_prefixes().is_empty())
    }

    async fn create_marker(&self, bucket: &str, prefix: &str, parents: bool) -> Result<()> {
        match self
            .client
            .put_object()
            .bucket(bucket)
            .key(prefix)
            .if_none_match("*")
            .content_length(0)
            .body(ByteStream::from_static(b""))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // A concurrent creator (or an SDK retry after a lost successful
            // reply) may have won. -p accepts it, but never overwrites its data.
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                if parents {
                    Ok(())
                } else {
                    Err(already_exists(prefix))
                }
            }
            Err(error) => Err(operation_error("PutObject", &error, &self.redactions)),
        }
    }
}

fn already_exists(prefix: &str) -> anyhow::Error {
    NonRetryableError(format!("remote directory already exists: {prefix:?}")).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::Connection, core::JobState, daemon::Daemon, s3::connection_tests::http_client,
    };

    const EMPTY: &str = "<ListBucketResult></ListBucketResult>";
    fn listing(key: &str) -> String {
        format!("<ListBucketResult><Contents><Key>{key}</Key><Size>0</Size></Contents></ListBucketResult>")
    }
    fn response(status: u16, body: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 {status} Response\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()), body.as_bytes().to_vec())
    }
    fn assert_markers(requests: &[String], paths: &[&str]) {
        let puts = requests
            .iter()
            .filter(|request| request.starts_with("PUT "))
            .collect::<Vec<_>>();
        assert_eq!(puts.len(), paths.len(), "{requests:?}");
        for (request, path) in puts.iter().zip(paths) {
            let target = request.split_whitespace().nth(1).unwrap();
            let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
            assert_eq!(url.path(), *path);
            let headers = request.to_ascii_lowercase();
            assert!(headers.contains("\r\nif-none-match: *\r\n"), "{request}");
            assert!(headers.contains("\r\ncontent-length: 0\r\n"), "{request}");
        }
    }
    fn prefixes(requests: &[String]) -> Vec<String> {
        requests
            .iter()
            .filter(|request| request.starts_with("GET "))
            .map(|request| {
                let path = request.split_whitespace().nth(1).unwrap();
                let url = url::Url::parse(&format!("http://localhost{path}")).unwrap();
                assert!(url
                    .query_pairs()
                    .any(|(key, value)| key == "max-keys" && value == "1"));
                url.query_pairs()
                    .find(|(key, _)| key == "prefix")
                    .unwrap()
                    .1
                    .into_owned()
            })
            .collect()
    }

    #[tokio::test]
    async fn parents_create_empty_conditional_markers_in_order_without_changing_pwd() {
        let (connection, requests) = http_client(vec![
            response(200, ""),
            response(200, EMPTY),
            response(200, ""),
            response(200, EMPTY),
            response(200, ""),
            response(200, EMPTY),
            response(200, ""),
        ])
        .await;
        connection.create_directory("a/b/c", true).await.unwrap();
        assert_eq!(connection.pwd().unwrap(), "/example/");
        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("HEAD /example"));
        assert_markers(
            &requests,
            &["/example/a/", "/example/a/b/", "/example/a/b/c/"],
        );
        assert_eq!(prefixes(&requests), vec!["a/", "a/b/", "a/b/c/"]);
    }

    #[tokio::test]
    async fn plain_mkdir_requires_a_parent_but_accepts_an_implicit_one() {
        for parent_exists in [false, true] {
            let parent = if parent_exists {
                listing("a/sibling.txt")
            } else {
                EMPTY.into()
            };
            let mut responses = vec![
                response(200, ""),
                response(200, EMPTY),
                response(200, &parent),
            ];
            if parent_exists {
                responses.push(response(200, ""));
            }
            let (connection, requests) = http_client(responses).await;
            let result = connection.create_directory("a/new", false).await;
            if parent_exists {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(error.is::<NonRetryableError>());
                assert!(error.to_string().contains("no such parent directory"));
            }
            let requests = requests.await.unwrap();
            assert_markers(
                &requests,
                if parent_exists {
                    &["/example/a/new/"]
                } else {
                    &[]
                },
            );
            assert_eq!(prefixes(&requests), vec!["a/new/", "a/"]);
        }
    }

    #[tokio::test]
    async fn existing_markers_and_implicit_directories_are_accepted_only_with_parents() {
        for parents in [false, true] {
            for existing in [
                listing("photos/"),
                listing("photos/existing.jpg"),
                listing("photos/").replace("<Size>0</Size>", "<Size>137</Size>"),
            ] {
                let (connection, requests) =
                    http_client(vec![response(200, ""), response(200, &existing)]).await;
                let result = connection.create_directory("photos", parents).await;
                if parents {
                    result.unwrap();
                } else {
                    let error = result.unwrap_err();
                    assert!(error.is::<NonRetryableError>());
                    assert!(error.to_string().contains("already exists"));
                }
                assert_markers(&requests.await.unwrap(), &[]);
            }
        }
    }

    #[tokio::test]
    async fn parents_preserve_implicit_ancestors_and_only_write_missing_markers() {
        let (connection, requests) = http_client(vec![
            response(200, ""),
            response(200, &listing("a/old.txt")),
            response(200, EMPTY),
            response(200, ""),
            response(200, EMPTY),
            response(200, ""),
        ])
        .await;
        connection.create_directory("a/b/c", true).await.unwrap();
        assert_markers(
            &requests.await.unwrap(),
            &["/example/a/b/", "/example/a/b/c/"],
        );
    }

    #[tokio::test]
    async fn concurrent_marker_creation_never_falls_back_to_unconditional_overwrite() {
        for parents in [false, true] {
            let (connection, requests) = http_client(vec![
                response(200, ""),
                response(200, EMPTY),
                response(412, "<Error><Code>PreconditionFailed</Code></Error>"),
            ])
            .await;
            let result = connection.create_directory("new", parents).await;
            if parents {
                result.unwrap();
            } else {
                assert!(result.unwrap_err().is::<NonRetryableError>());
            }
            assert_markers(&requests.await.unwrap(), &["/example/new/"]);
        }
    }

    #[tokio::test]
    async fn bucket_boundaries_are_validated_and_buckets_are_never_created() {
        let (connection, requests) = http_client(vec![]).await;
        assert!(connection
            .create_directory("/", true)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        assert!(requests.await.unwrap().is_empty());
        for parents in [false, true] {
            let (connection, requests) = http_client(vec![response(200, "")]).await;
            let result = connection.create_directory("/example/", parents).await;
            assert_eq!(result.is_ok(), parents);
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("HEAD /example"));
            let (connection, requests) = http_client(vec![response(404, "")]).await;
            assert!(connection
                .create_directory("/missing/a/b", parents)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            let requests = requests.await.unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("HEAD /missing"));
        }
    }

    #[tokio::test]
    async fn marker_paths_are_relative_to_remote_pwd_and_encoded_without_touching_bare_file_keys() {
        let (mut connection, requests) = http_client(vec![
            response(200, ""),
            response(200, EMPTY),
            response(200, ""),
        ])
        .await;
        connection.location = connection.location.resolve("photos");
        connection
            .create_directory("../file name", false)
            .await
            .unwrap();
        assert_eq!(connection.pwd().unwrap(), "/example/photos/");
        let requests = requests.await.unwrap();
        // A same-named bare object is distinct from its trailing-slash directory.
        assert_eq!(prefixes(&requests), vec!["file name/"]);
        assert_markers(&requests, &["/example/file%20name/"]);
    }

    #[tokio::test]
    async fn directory_errors_classify_permanent_failures_and_keep_redacted_diagnostics() {
        for (status, code, retryable) in [
            (403, "AccessDenied", false),
            (400, "InvalidRequest", false),
            (501, "NotImplemented", false),
            (503, "SlowDown", true),
            (429, "TooManyRequests", true),
            (409, "ConditionalRequestConflict", true),
            (400, "RequestTimeout", true),
        ] {
            let body = format!(
                "<Error><Code>{code}</Code><Message>test-secret&#10;details</Message></Error>"
            );
            let (mut connection, requests) = http_client(vec![
                response(200, ""),
                response(200, EMPTY),
                response(status, &body),
            ])
            .await;
            connection.redactions = vec!["test-secret".into()];
            let error = connection.create_directory("new", true).await.unwrap_err();
            assert_eq!(!error.is::<NonRetryableError>(), retryable, "{error}");
            let message = error.to_string();
            assert!(message.contains(&format!("HTTP {status}")), "{message}");
            assert!(message.contains("[redacted]"), "{message}");
            assert!(
                !message.contains("test-secret") && !message.contains('\n'),
                "{message}"
            );
            assert_markers(&requests.await.unwrap(), &["/example/new/"]);
        }
        for fail_on_head in [true, false] {
            let mut responses = Vec::new();
            if !fail_on_head {
                responses.push(response(200, ""));
            }
            responses.push(response(403, ""));
            let (connection, requests) = http_client(responses).await;
            assert!(connection
                .create_directory("new", true)
                .await
                .unwrap_err()
                .is::<NonRetryableError>());
            assert_markers(&requests.await.unwrap(), &[]);
        }
    }

    async fn command(app: &mut Daemon, source: &str) -> u64 {
        let id = app.command(source)[0]
            .strip_prefix("JOB ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while app.has_running_jobs() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("command did not finish");
        assert_eq!(
            app.jobs.jobs[&id].state,
            JobState::Completed,
            "{:?}",
            app.jobs.jobs[&id].output.read_all()
        );
        id
    }

    #[tokio::test]
    async fn parents_resume_after_a_lost_successful_put_reply_without_duplicate_writes() {
        let (mut connection, requests) = http_client(vec![
            response(200, ""),
            response(200, &listing("photos/existing")),
            response(200, EMPTY),
            response(200, ""),
            response(200, EMPTY),
            (String::new(), Vec::new()), // marker created, acknowledgement lost
            response(200, ""),
            response(200, &listing("photos/existing")),
            response(200, &listing("photos/new/")),
            response(200, &listing("photos/new/leaf/")),
        ])
        .await;
        connection.location = connection.location.resolve("photos");
        let mut app = Daemon::bootstrap().unwrap();
        *app.connection.lock().await = Some(Box::new(connection));
        command(&mut app, "mkdir -p new/leaf").await;
        assert_eq!(
            app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
            "/example/photos/"
        );
        let requests = requests.await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.starts_with("HEAD "))
                .count(),
            2
        );
        assert_markers(
            &requests,
            &["/example/photos/new/", "/example/photos/new/leaf/"],
        );
    }

    #[tokio::test]
    async fn mkdir_refreshes_completion_and_new_empty_directories_support_cd_and_ls() {
        let (connection, requests) = http_client(vec![response(200, EMPTY),
            response(200, ""), response(200, EMPTY), response(200, ""), response(200, EMPTY), response(200, ""),
            response(200, "<ListBucketResult><CommonPrefixes><Prefix>new/</Prefix></CommonPrefixes></ListBucketResult>"),
            response(200, &listing("new/sub/")), response(200, &listing("new/sub/")),
        ]).await;
        let mut app = Daemon::bootstrap().unwrap();
        let local = app.local_directory.clone();
        let options = crate::cache::CacheOptions::from_settings(&app.settings).unwrap();
        *app.connection.lock().await = Some(Box::new(crate::cache::CachedConnection::new(
            Box::new(connection),
            options,
        )));
        assert!(app
            .connection
            .lock()
            .await
            .as_ref()
            .unwrap()
            .complete_directories("new")
            .await
            .unwrap()
            .candidates
            .is_empty());
        command(&mut app, "mkdir -p new/sub").await;
        assert_eq!(app.local_directory, local);
        assert_eq!(
            app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
            "/example/"
        );
        assert_eq!(
            app.connection
                .lock()
                .await
                .as_ref()
                .unwrap()
                .complete_directories("new")
                .await
                .unwrap()
                .candidates,
            vec!["new/"]
        );
        command(&mut app, "cd new/sub").await;
        assert_eq!(
            app.connection.lock().await.as_ref().unwrap().pwd().unwrap(),
            "/example/new/sub/"
        );
        let id = command(&mut app, "ls").await;
        assert!(app.jobs.jobs[&id].output.read_all().is_empty());
        assert_markers(
            &requests.await.unwrap(),
            &["/example/new/", "/example/new/sub/"],
        );
    }
}
