/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Shared framed-RPC fixture for binary unit tests only.
use super::Connection;
use std::{os::unix::net::UnixStream, time::Duration};
use xfer::ipc::{self, pb, Op, Reply};

pub(super) fn rpc_pair(
    count: usize,
    mut handle: impl FnMut(Op) -> Reply + Send + 'static,
) -> (Connection, std::thread::JoinHandle<UnixStream>) {
    let (client, mut server) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    server
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let worker = std::thread::spawn(move || {
        for expected_id in 1..=count as u64 {
            let request: pb::Request = ipc::read_frame(&mut server).unwrap().unwrap();
            assert_eq!(request.version, ipc::VERSION);
            assert_eq!(request.id, expected_id);
            let result = handle(request.operation.unwrap());
            ipc::write_frame(
                &mut server,
                &pb::Response {
                    version: ipc::VERSION,
                    id: request.id,
                    result: Some(result),
                },
            )
            .unwrap();
        }
        server
    });
    (Connection::new(client), worker)
}
