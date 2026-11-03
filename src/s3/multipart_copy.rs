/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Server-side multipart copy. Source is retained unless the caller verifies it.
use super::{
    operation_error,
    rename::strong_tag,
    upload::{part_size, MultipartGuard},
    S3Connection,
};
use crate::error::NonRetryableError;
use anyhow::Result;
use aws_sdk_s3::{
    operation::head_object::HeadObjectOutput,
    types::{CompletedMultipartUpload, CompletedPart},
};

impl S3Connection {
    #[allow(clippy::too_many_arguments)]
    #[allow(deprecated)]
    pub(super) async fn multipart_copy(
        &self,
        bucket: &str,
        key: &str,
        target_bucket: &str,
        target_key: &str,
        copy_source: &str,
        tag: &str,
        before: &HeadObjectOutput,
        size: u64,
        overwrite: bool,
    ) -> Result<String> {
        // Multipart creation doesn't inherit metadata or tags like CopyObject.
        // Read tags from the same version where available; lack of permission is
        // an error rather than silently dropping metadata.
        let tags = self
            .client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(
                before
                    .version_id()
                    .filter(|id| !id.is_empty() && *id != "null")
                    .map(str::to_owned),
            )
            .send()
            .await
            .map_err(|e| operation_error("GetObjectTagging", &e, &self.redactions))?;
        let encoded = {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            for tag in tags.tag_set() {
                serializer.append_pair(tag.key(), tag.value());
            }
            serializer.finish()
        };
        let created = self
            .client
            .create_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .set_cache_control(before.cache_control().map(str::to_owned))
            .set_content_disposition(before.content_disposition().map(str::to_owned))
            .set_content_encoding(before.content_encoding().map(str::to_owned))
            .set_content_language(before.content_language().map(str::to_owned))
            .set_content_type(before.content_type().map(str::to_owned))
            .set_expires(before.expires().cloned())
            .set_metadata(before.metadata().cloned())
            .set_storage_class(before.storage_class().cloned())
            .set_website_redirect_location(before.website_redirect_location().map(str::to_owned))
            .set_tagging((!encoded.is_empty()).then_some(encoded))
            .send()
            .await
            .map_err(|e| operation_error("CreateMultipartUpload", &e, &self.redactions))?;
        let id = created
            .upload_id()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                NonRetryableError(
                    "S3 multipart copy returned no upload ID; incomplete upload may remain".into(),
                )
            })?
            .to_owned();
        let mut guard = MultipartGuard {
            client: self.client.clone(),
            bucket: target_bucket.into(),
            key: target_key.into(),
            id: Some(id.clone()),
        };
        let mut parts = Vec::new();
        let mut offset = 0;
        while offset < size {
            let end = (offset + part_size(size)).min(size) - 1;
            let number = parts.len() as i32 + 1;
            let timeout = aws_config::timeout::TimeoutConfig::builder()
                .disable_operation_timeout()
                .read_timeout(std::time::Duration::from_secs(120))
                .build();
            let result = self
                .client
                .upload_part_copy()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(&id)
                .part_number(number)
                .copy_source(copy_source)
                .copy_source_if_match(tag)
                .copy_source_range(format!("bytes={offset}-{end}"))
                .customize()
                .config_override(aws_sdk_s3::config::Builder::new().timeout_config(timeout))
                .send()
                .await
                .map_err(|e| operation_error("UploadPartCopy", &e, &self.redactions))?;
            let part_tag = result
                .copy_part_result()
                .and_then(|r| r.e_tag())
                .filter(|tag| strong_tag(tag))
                .ok_or_else(|| {
                    NonRetryableError(
                        "S3 copy part has no valid ETag; source was not deleted".into(),
                    )
                })?;
            parts.push(
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(part_tag)
                    .build(),
            );
            offset = end + 1;
        }
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .upload_id(&id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            );
        if !overwrite {
            complete = complete.if_none_match("*");
        }
        let timeout = aws_config::timeout::TimeoutConfig::builder()
            .disable_operation_timeout()
            .read_timeout(std::time::Duration::from_secs(120))
            .build();
        let result = complete.customize().config_override(aws_sdk_s3::config::Builder::new().timeout_config(timeout))
            .send().await.map_err(|e| NonRetryableError(format!("S3 multipart copy completion failed or outcome is unknown; source was not deleted; inspect destination: {}", operation_error("CompleteMultipartUpload", &e, &self.redactions))))?;
        guard.id = None;
        let tag = result
            .e_tag()
            .filter(|tag| strong_tag(tag))
            .ok_or_else(|| {
                NonRetryableError(
                    "S3 multipart copy has no ETag; source was not deleted; inspect destination"
                        .into(),
                )
            })?;
        Ok(tag.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Connection, s3::connection_tests::http_client};
    fn response(status: u16, body: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 {status} Response\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()), body.as_bytes().to_vec())
    }
    fn head(size: u64, tag: &str) -> (String, Vec<u8>) {
        (format!("HTTP/1.1 200 OK\r\ncontent-length: {size}\r\netag: {tag}\r\ncontent-type: text/plain\r\nx-amz-meta-custom: value\r\nconnection: close\r\n\r\n"), Vec::new())
    }
    #[tokio::test]
    async fn large_move_copies_parts_preserves_metadata_and_verifies_before_delete() {
        for overwrite in [false, true] {
            let size = super::super::upload::SINGLE_LIMIT + 1;
            let count = size.div_ceil(part_size(size));
            let mut responses = vec![head(size, "\"source\""),
                response(200, "<Tagging><TagSet><Tag><Key>team</Key><Value>blue</Value></Tag></TagSet></Tagging>"),
                response(200, "<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>")];
            responses.extend((0..count).map(|_| {
                response(
                    200,
                    "<CopyPartResult><ETag>&quot;part&quot;</ETag></CopyPartResult>",
                )
            }));
            responses.extend([response(200, "<CompleteMultipartUploadResult><ETag>&quot;copy&quot;</ETag></CompleteMultipartUploadResult>"), head(size, "\"copy\""), head(size, "\"source\""), response(204, "")]);
            let (connection, requests) = http_client(responses).await;
            connection
                .rename_file("source", "dest", overwrite)
                .await
                .unwrap();
            let requests = requests.await.unwrap();
            assert!(requests[1].contains("tagging"));
            let create = requests[2].to_ascii_lowercase();
            assert!(create.contains("x-amz-meta-custom: value"));
            assert!(create.contains("x-amz-tagging: team=blue"));
            for (i, req) in requests[3..3 + count as usize].iter().enumerate() {
                let req = req.to_ascii_lowercase();
                let start = i as u64 * part_size(size);
                let end = (start + part_size(size)).min(size) - 1;
                assert!(
                    req.contains(&format!("x-amz-copy-source-range: bytes={start}-{end}")),
                    "{req}"
                );
                assert!(req.contains("x-amz-copy-source-if-match: \"source\""));
            }
            let complete = &requests[3 + count as usize];
            assert_eq!(
                complete.to_ascii_lowercase().contains("if-none-match: *"),
                !overwrite
            );
            assert!(requests
                .last()
                .unwrap()
                .starts_with("DELETE /example/source"));
        }
    }

    #[tokio::test]
    async fn part_failure_aborts_without_deleting_source() {
        let size = super::super::upload::SINGLE_LIMIT + 1;
        let (connection, requests) = http_client(vec![head(size, "\"source\""),
            response(200, "<Tagging><TagSet/></Tagging>"),
            response(200, "<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>"),
            response(412, "<Error><Code>PreconditionFailed</Code></Error>"), response(204, "")]).await;
        assert!(connection
            .rename_file("source", "dest", false)
            .await
            .is_err());
        let requests = tokio::time::timeout(std::time::Duration::from_secs(5), requests)
            .await
            .unwrap()
            .unwrap();
        assert!(requests
            .last()
            .unwrap()
            .starts_with("DELETE /example/dest?"));
        assert!(requests
            .iter()
            .all(|r| !r.starts_with("DELETE /example/source")));
    }
    #[tokio::test]
    async fn failed_completion_and_changed_source_never_delete_source() {
        let size = super::super::upload::SINGLE_LIMIT + 1;
        let count = size.div_ceil(part_size(size));
        for conflict in [true, false] {
            let mut responses = vec![head(size, "\"source\""), response(200, "<Tagging><TagSet/></Tagging>"), response(200, "<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>")];
            responses.extend((0..count).map(|_| {
                response(
                    200,
                    "<CopyPartResult><ETag>&quot;part&quot;</ETag></CopyPartResult>",
                )
            }));
            if conflict {
                responses.push(response(
                    412,
                    "<Error><Code>PreconditionFailed</Code></Error>",
                ));
                responses.push(response(204, ""));
            } else {
                responses.push(response(200, "<CompleteMultipartUploadResult><ETag>&quot;copy&quot;</ETag></CompleteMultipartUploadResult>"));
                responses.push(head(size, "\"copy\""));
                responses.push(head(size, "\"changed\""));
            }
            let (connection, requests) = http_client(responses).await;
            assert!(connection
                .rename_file("source", "dest", false)
                .await
                .is_err());
            let requests = tokio::time::timeout(std::time::Duration::from_secs(5), requests)
                .await
                .unwrap()
                .unwrap();
            assert!(requests
                .iter()
                .all(|r| !r.starts_with("DELETE /example/source")));
            if conflict {
                assert!(requests
                    .last()
                    .unwrap()
                    .starts_with("DELETE /example/dest?"));
            }
        }
    }
}
