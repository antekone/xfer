/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! An in-process SFTP v3 peer with independent delayed replies. Sleeping once
//! per response in a serial proxy would hide pipelining, not model network RTT.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex as AsyncMutex,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Default)]
struct Behavior {
    latency: Duration,
    reorder: bool,
    short: Option<usize>,
    legacy_limits: bool,
    eof_at: Option<u64>,
    oversized: bool,
    reject_write: Option<u64>,
    fail_close: bool,
    changed: bool,
    on_write: Option<Arc<dyn Fn() + Send + Sync>>,
}
struct State {
    data: Mutex<Vec<u8>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    max_read: AtomicUsize,
    max_write: AtomicUsize,
    stats: AtomicUsize,
    completed: Mutex<Vec<u64>>,
}
struct Peer {
    client: Option<Client>,
    state: Arc<State>,
    server: JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn put32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn string(out: &mut Vec<u8>, value: &[u8]) {
    put32(out, value.len() as u32);
    out.extend_from_slice(value);
}
fn take32(data: &mut &[u8]) -> u32 {
    let value = u32::from_be_bytes(data[..4].try_into().unwrap());
    *data = &data[4..];
    value
}
fn take64(data: &mut &[u8]) -> u64 {
    let value = u64::from_be_bytes(data[..8].try_into().unwrap());
    *data = &data[8..];
    value
}
fn take_string<'a>(data: &mut &'a [u8]) -> &'a [u8] {
    let size = take32(data) as usize;
    let value = &data[..size];
    *data = &data[size..];
    value
}
fn response(kind: u8, id: u32) -> Vec<u8> {
    let mut out = vec![kind];
    put32(&mut out, id);
    out
}
fn status(id: u32, code: u32) -> Vec<u8> {
    let mut out = response(101, id);
    put32(&mut out, code);
    string(&mut out, b"");
    string(&mut out, b"");
    out
}
async fn packet(out: &mut (impl AsyncWriteExt + Unpin), body: &[u8]) -> std::io::Result<()> {
    out.write_all(&(body.len() as u32).to_be_bytes()).await?;
    out.write_all(body).await?;
    out.flush().await
}
impl Peer {
    async fn new(initial: Vec<u8>, behavior: Behavior) -> Self {
        let state = Arc::new(State {
            data: Mutex::new(initial.clone()),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            max_read: AtomicUsize::new(0),
            max_write: AtomicUsize::new(0),
            stats: AtomicUsize::new(0),
            completed: Mutex::new(Vec::new()),
        });
        let (client, server) = tokio::io::duplex(128 * 1024);
        let (mut reader, writer) = tokio::io::split(server);
        let writer = Arc::new(AsyncMutex::new(writer));
        let shared = state.clone();
        let server = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut exists = !initial.is_empty();
            while let Ok(length) = reader.read_u32().await {
                let length = length as usize;
                assert!(length <= 1024 * 1024);
                let mut body = vec![0; length];
                if reader.read_exact(&mut body).await.is_err() {
                    break;
                }
                let kind = body[0];
                if kind == 1 {
                    let mut version = vec![2];
                    put32(&mut version, 3);
                    if !behavior.legacy_limits {
                        string(&mut version, b"limits@openssh.com");
                        string(&mut version, b"1");
                    }
                    if packet(&mut *writer.lock().await, &version).await.is_err() {
                        break;
                    }
                    continue;
                }
                let mut data = &body[1..];
                let id = take32(&mut data);
                let answer = match kind {
                    200 => {
                        assert_eq!(take_string(&mut data), b"limits@openssh.com");
                        let mut out = response(201, id);
                        for n in [256 * 1024, CHUNK as u64, CHUNK as u64, 256] {
                            put64(&mut out, n);
                        }
                        out
                    }
                    3 => {
                        take_string(&mut data);
                        let flags = take32(&mut data);
                        if flags & 0x20 != 0 && exists {
                            status(id, 4)
                        } else {
                            if flags & 0x10 != 0 {
                                shared.data.lock().unwrap().clear();
                            }
                            exists = true;
                            let mut out = response(102, id);
                            string(&mut out, b"H");
                            out
                        }
                    }
                    8 => {
                        let mut out = response(105, id);
                        put32(&mut out, 13);
                        put64(&mut out, shared.data.lock().unwrap().len() as u64);
                        put32(&mut out, 0o100644);
                        put32(&mut out, 7);
                        let changed =
                            shared.stats.fetch_add(1, Ordering::Relaxed) != 0 && behavior.changed;
                        put32(&mut out, if changed { 8 } else { 7 });
                        out
                    }
                    4 => status(id, if behavior.fail_close { 4 } else { 0 }),
                    5 | 6 => {
                        assert_eq!(take_string(&mut data), b"H");
                        let offset = take64(&mut data);
                        let out = if kind == 5 {
                            let requested = take32(&mut data) as usize;
                            shared.max_read.fetch_max(requested, Ordering::Relaxed);
                            let bytes = shared.data.lock().unwrap();
                            if behavior.eof_at.is_some_and(|end| offset >= end)
                                || offset >= bytes.len() as u64
                            {
                                status(id, 1)
                            } else {
                                let length = if behavior.oversized {
                                    requested + 1
                                } else {
                                    requested.min(behavior.short.unwrap_or(usize::MAX))
                                };
                                let end = (offset as usize + length).min(bytes.len());
                                let mut out = response(103, id);
                                string(&mut out, &bytes[offset as usize..end]);
                                out
                            }
                        } else {
                            let bytes = take_string(&mut data);
                            shared.max_write.fetch_max(bytes.len(), Ordering::Relaxed);
                            if behavior.reject_write.is_some_and(|start| offset >= start) {
                                status(id, 4)
                            } else {
                                if offset == 0 {
                                    if let Some(action) = &behavior.on_write {
                                        action();
                                    }
                                }
                                let mut stored = shared.data.lock().unwrap();
                                let end = offset as usize + bytes.len();
                                if stored.len() < end {
                                    stored.resize(end, 0);
                                }
                                stored[offset as usize..end].copy_from_slice(bytes);
                                status(id, 0)
                            }
                        };
                        let state = shared.clone();
                        let writer = writer.clone();
                        let mut delay = behavior.latency;
                        if behavior.reorder && offset.is_multiple_of((CHUNK * 4) as u64) {
                            delay *= 3;
                        }
                        let active = state.active.fetch_add(1, Ordering::Relaxed) + 1;
                        state.peak.fetch_max(active, Ordering::Relaxed);
                        tasks.spawn(async move {
                            tokio::time::sleep(delay).await;
                            let _ = packet(&mut *writer.lock().await, &out).await;
                            state.completed.lock().unwrap().push(offset);
                            state.active.fetch_sub(1, Ordering::Relaxed);
                        });
                        while tasks.try_join_next().is_some() {}
                        continue;
                    }
                    _ => panic!("unexpected SFTP operation {kind}"),
                };
                if packet(&mut *writer.lock().await, &answer).await.is_err() {
                    break;
                }
            }
            tasks.abort_all();
        });
        let (input, output) = tokio::io::split(client);
        let client = Client::new(
            output,
            input,
            openssh_sftp_client::SftpOptions::new().flush_interval(Duration::ZERO),
        )
        .await
        .unwrap();
        Self {
            client: Some(client),
            state,
            server,
        }
    }
    fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }
    async fn close(mut self) {
        let client = self.client.take().unwrap();
        tokio::time::timeout(Duration::from_secs(3), client.close())
            .await
            .unwrap()
            .unwrap();
    }
}
struct Sink(Vec<u8>);
impl DownloadSink for Sink {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}
fn content(length: usize) -> Vec<u8> {
    (0..length).map(|i| (i % 251) as u8).collect()
}
fn expected(length: usize) -> RemoteFile {
    RemoteFile {
        size: length as u64,
        revision: Some("7".into()),
    }
}

#[tokio::test]
async fn downloads_pipeline_short_and_out_of_order_replies_without_gaps_and_resume_exactly() {
    let data = content(CHUNK * 12 + 117);
    let peer = Peer::new(
        data.clone(),
        Behavior {
            latency: Duration::from_millis(2),
            short: Some(8191),
            reorder: true,
            ..Default::default()
        },
    )
    .await;
    let offset = 177u64;
    let mut sink = Sink(Vec::new());
    download_on(
        peer.client(),
        "/file",
        offset,
        &expected(data.len()),
        &mut sink,
        8,
    )
    .await
    .unwrap();
    assert_eq!(sink.0, data[offset as usize..]);
    assert!((2..=8).contains(&peer.state.peak.load(Ordering::Relaxed)));
    assert!(peer
        .state
        .completed
        .lock()
        .unwrap()
        .windows(2)
        .any(|v| v[0] > v[1]));
    peer.close().await;
}
#[tokio::test]
async fn uploads_pipeline_offset_writes_and_wait_for_out_of_order_acknowledgements() {
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("input");
    let data = content(CHUNK * 12 + 117);
    std::fs::write(&local, &data).unwrap();
    let peer = Peer::new(
        Vec::new(),
        Behavior {
            latency: Duration::from_millis(4),
            reorder: true,
            ..Default::default()
        },
    )
    .await;
    upload_on(peer.client(), &local, "/file", false, 8)
        .await
        .unwrap();
    assert_eq!(*peer.state.data.lock().unwrap(), data);
    assert!((2..=8).contains(&peer.state.peak.load(Ordering::Relaxed)));
    assert!(peer
        .state
        .completed
        .lock()
        .unwrap()
        .windows(2)
        .any(|v| v[0] > v[1]));
    peer.close().await;
}
#[tokio::test]
async fn latency_benchmark_compares_serial_and_pipelined_downloads_and_uploads() {
    let data = content(CHUNK * 64);
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("input");
    std::fs::write(&local, &data).unwrap();
    for upload in [false, true] {
        let mut times = Vec::new();
        for window in [1, 16, 64] {
            let peer = Peer::new(
                if upload { Vec::new() } else { data.clone() },
                Behavior {
                    latency: Duration::from_millis(65),
                    ..Default::default()
                },
            )
            .await;
            let started = Instant::now();
            if upload {
                upload_on(peer.client(), &local, "/file", false, window)
                    .await
                    .unwrap();
                assert_eq!(*peer.state.data.lock().unwrap(), data);
            } else {
                let mut sink = Sink(Vec::new());
                download_on(
                    peer.client(),
                    "/file",
                    0,
                    &expected(data.len()),
                    &mut sink,
                    window,
                )
                .await
                .unwrap();
                assert_eq!(sink.0, data);
            }
            let elapsed = started.elapsed();
            times.push(elapsed);
            eprintln!(
                "SFTP {}: window={window}, 65ms reply latency, {:.2} MiB/s ({elapsed:?})",
                if upload { "upload" } else { "download" },
                (data.len() as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
            );
            assert!(peer.state.peak.load(Ordering::Relaxed) <= window);
            peer.close().await;
        }
        assert!(
            times[1] * 3 < times[0],
            "pipelining did not overcome latency: {times:?}"
        );
    }
}
#[tokio::test]
async fn invalid_or_changed_downloads_stop_permanently_and_require_a_fresh_session() {
    let data = content(CHUNK * 5);
    for behavior in [
        Behavior {
            oversized: true,
            ..Default::default()
        },
        Behavior {
            eof_at: Some(CHUNK as u64),
            ..Default::default()
        },
        Behavior {
            changed: true,
            ..Default::default()
        },
        Behavior {
            fail_close: true,
            ..Default::default()
        },
    ] {
        let peer = Peer::new(data.clone(), behavior).await;
        let mut sink = Sink(Vec::new());
        let error = download_on(
            peer.client(),
            "/file",
            0,
            &expected(data.len()),
            &mut sink,
            4,
        )
        .await
        .unwrap_err();
        assert!(error.is::<crate::error::NonRetryableError>(), "{error:#}");
        assert!(error.is::<DiscardSession>());
        assert_eq!(sink.0, data[..sink.0.len()]);
        // A discarded production session is dropped, not reused; this peer
        // guard aborts outstanding fixture tasks when it leaves scope.
    }
}
#[tokio::test]
async fn upload_failures_close_acknowledgements_local_changes_and_clobber_are_safe() {
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("input");
    let data = content(CHUNK * 4);
    std::fs::write(&local, &data).unwrap();
    for behavior in [
        Behavior {
            reject_write: Some(CHUNK as u64),
            ..Default::default()
        },
        Behavior {
            fail_close: true,
            ..Default::default()
        },
    ] {
        let peer = Peer::new(Vec::new(), behavior).await;
        let error = upload_on(peer.client(), &local, "/file", false, 4)
            .await
            .unwrap_err();
        assert!(error.is::<crate::error::NonRetryableError>());
        assert!(error.is::<DiscardSession>());
    }
    let path = local.clone();
    let action = Arc::new(move || {
        std::fs::write(&path, b"changed").unwrap();
    });
    let peer = Peer::new(
        Vec::new(),
        Behavior {
            latency: Duration::from_millis(3),
            on_write: Some(action),
            ..Default::default()
        },
    )
    .await;
    let error = upload_on(peer.client(), &local, "/file", false, 4)
        .await
        .unwrap_err();
    assert!(error.is::<crate::error::NonRetryableError>());
    assert!(error.is::<DiscardSession>());
    assert!(format!("{error:#}").contains("local file changed"));
    std::fs::write(&local, &data).unwrap();
    let peer = Peer::new(b"keep".to_vec(), Behavior::default()).await;
    let error = upload_on(peer.client(), &local, "/file", false, 4)
        .await
        .unwrap_err();
    assert!(!error.is::<DiscardSession>());
    assert_eq!(*peer.state.data.lock().unwrap(), b"keep");
    upload_on(peer.client(), &local, "/file", true, 4)
        .await
        .unwrap();
    assert_eq!(*peer.state.data.lock().unwrap(), data);
    peer.close().await;
}

#[tokio::test]
async fn legacy_server_limits_clamp_requests_without_losing_offsets() {
    let data = content(CHUNK * 4 + 7);
    let peer = Peer::new(
        data.clone(),
        Behavior {
            legacy_limits: true,
            reorder: true,
            latency: Duration::from_millis(1),
            ..Default::default()
        },
    )
    .await;
    let mut sink = Sink(Vec::new());
    download_on(
        peer.client(),
        "/file",
        0,
        &expected(data.len()),
        &mut sink,
        8,
    )
    .await
    .unwrap();
    assert_eq!(sink.0, data);
    assert!(peer.state.max_read.load(Ordering::Relaxed) <= 20480);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("file");
    std::fs::write(&path, &data).unwrap();
    upload_on(peer.client(), &path, "/file", true, 8)
        .await
        .unwrap();
    assert_eq!(*peer.state.data.lock().unwrap(), data);
    assert!(peer.state.max_write.load(Ordering::Relaxed) <= 20480);
    peer.close().await;
}
#[tokio::test]
async fn cancellation_drops_pending_work_and_uploads_reject_links_and_special_files() {
    let data = content(CHUNK * 64);
    let peer = Peer::new(
        data.clone(),
        Behavior {
            latency: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .await;
    let mut sink = Sink(Vec::new());
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        download_on(
            peer.client(),
            "/file",
            0,
            &expected(data.len()),
            &mut sink,
            8
        )
    )
    .await
    .is_err());
    assert!(sink.0.is_empty());
    assert!((2..=8).contains(&peer.state.peak.load(Ordering::Relaxed)));
    drop(peer);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("file");
    std::fs::write(&path, &data).unwrap();
    let peer = Peer::new(
        Vec::new(),
        Behavior {
            latency: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .await;
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        upload_on(peer.client(), &path, "/file", false, 8)
    )
    .await
    .is_err());
    assert!((2..=8).contains(&peer.state.peak.load(Ordering::Relaxed)));
    drop(peer);
    let peer = Peer::new(Vec::new(), Behavior::default()).await;
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(upload_on(peer.client(), &link, "/file", false, 8)
        .await
        .unwrap_err()
        .is::<crate::error::NonRetryableError>());
    assert!(upload_on(peer.client(), root.path(), "/file", false, 8)
        .await
        .unwrap_err()
        .is::<crate::error::NonRetryableError>());
    let fifo = root.path().join("fifo");
    let name = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: the CString owns a valid NUL-terminated path in this private fixture.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    assert!(upload_on(peer.client(), &fifo, "/file", false, 8)
        .await
        .unwrap_err()
        .is::<crate::error::NonRetryableError>());
    assert!(peer.state.data.lock().unwrap().is_empty());
    peer.close().await;
}
