//! Bounded, offset-based SFTP pipelines. Clone a remote handle (not a session)
//! per chunk, keep requests concurrent, and deliver downloads in file order.
use super::{local_error, operation, permanent, unchanged, Client, DiscardSession};
use crate::transfer::{DownloadSink, RemoteFile};
use anyhow::Result;
use futures_util::{stream::FuturesOrdered, StreamExt};
use openssh_sftp_client::file::File as RemoteHandle;
use std::{
    fs::{Metadata, OpenOptions},
    io::{Read, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
use tokio::io::AsyncSeekExt;

const CHUNK: usize = 64 * 1024;

fn interrupted(result: Result<()>) -> Result<()> {
    result.map_err(|error| {
        let message = error.to_string();
        error.context(DiscardSession(message))
    })
}

/// Send a newly queued request now, even when FuturesOrdered has older ready
/// results. Otherwise buffered completions can delay refilling the wire window.
async fn prime<F: std::future::Future>(future: F) -> impl std::future::Future<Output = F::Output> {
    let mut future = Box::pin(future);
    let ready = std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await;
    async move {
        match ready {
            std::task::Poll::Ready(value) => value,
            std::task::Poll::Pending => future.await,
        }
    }
}

async fn read_chunk(mut handle: RemoteHandle, offset: u64, length: usize) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(length);
    while data.len() < length {
        // File::read advances by the requested length, not necessarily the
        // returned length. Seek explicitly after every short/clamped reply.
        handle
            .seek(SeekFrom::Start(offset + data.len() as u64))
            .await?;
        let remaining = length - data.len();
        let bytes = operation(handle.read(remaining as u32, Default::default()))
            .await?
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(|| permanent("remote file ended early; partial file was retained"))?;
        if bytes.len() > remaining {
            return Err(permanent("SFTP server returned too much data"));
        }
        data.extend_from_slice(&bytes);
    }
    Ok(data)
}

pub(super) async fn download_on(
    sftp: &Client,
    path: &str,
    offset: u64,
    expected: &RemoteFile,
    sink: &mut dyn DownloadSink,
    window: usize,
) -> Result<()> {
    if offset > expected.size {
        return Err(permanent("resume offset exceeds remote size"));
    }
    let mut file = operation(sftp.open(path)).await?;
    unchanged(&operation(file.metadata()).await?, expected)?;
    let chunk = CHUNK;
    let result: Result<()> = async {
        let mut reads = FuturesOrdered::new();
        let mut next = offset;
        let mut done = offset;
        while done < expected.size {
            while next < expected.size && reads.len() < window {
                let length = (expected.size - next).min(chunk as u64) as usize;
                reads.push_back(prime(read_chunk(file.clone(), next, length)).await);
                next += length as u64;
                tokio::task::yield_now().await;
            }
            let data = reads.next().await.expect("SFTP reads queued")?;
            sink.write(&data)?;
            done += data.len() as u64;
            tokio::task::yield_now().await;
        }
        unchanged(&operation(file.metadata()).await?, expected)?;
        operation(file.close()).await?;
        Ok(())
    }
    .await;
    // Preserve the root error's retryability, but never return a stream with
    // potentially abandoned reads/writes to the reusable session slot.
    interrupted(result)
}

async fn write_chunk(mut handle: RemoteHandle, offset: u64, data: Vec<u8>) -> Result<()> {
    handle.seek(SeekFrom::Start(offset)).await?;
    let mut data = [bytes::Bytes::from(data)];
    operation(handle.write_all_zero_copy(&mut data)).await
}

fn same_local_file(before: &Metadata, after: &Metadata) -> bool {
    before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

pub(super) async fn upload_on(
    sftp: &Client,
    local: &Path,
    remote: &str,
    overwrite: bool,
    window: usize,
) -> Result<()> {
    // Avoid opening a substituted symlink/FIFO between put's preflight and the
    // upload. Keep one descriptor and freeze its size for the entire attempt.
    let mut input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(local)
        .map_err(|error| local_error(local, error))?;
    let before = input
        .metadata()
        .map_err(|error| local_error(local, error))?;
    if !before.is_file() {
        return Err(permanent("put requires a regular local file"));
    }
    let mut options = sftp.options();
    options.write(true);
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let output = operation(options.open(remote)).await?;
    let chunk = CHUNK;
    let result: Result<()> = async {
        let mut writes = FuturesOrdered::new();
        let mut next = 0u64;
        while next < before.len() || !writes.is_empty() {
            while next < before.len() && writes.len() < window {
                let length = (before.len() - next).min(chunk as u64) as usize;
                let mut data = vec![0u8; length];
                input.read_exact(&mut data).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        permanent(
                            "local file changed during upload; remote partial file was retained",
                        )
                    } else {
                        local_error(local, error)
                    }
                })?;
                writes.push_back(prime(write_chunk(output.clone(), next, data)).await);
                next += length as u64;
                tokio::task::yield_now().await;
            }
            writes.next().await.expect("SFTP writes queued")?;
            tokio::task::yield_now().await;
        }
        let after = input
            .metadata()
            .map_err(|error| local_error(local, error))?;
        if !same_local_file(&before, &after) {
            return Err(permanent(
                "local file changed during upload; remote partial file was retained",
            ));
        }
        // All acknowledgements must succeed before closing/reporting success.
        operation(output.close()).await?;
        Ok(())
    }
    .await;
    interrupted(result)
}

#[cfg(test)]
mod tests;
