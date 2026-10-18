//! Versioned, bounded Protobuf IPC shared by all frontends. The transport only
//! needs Read + Write; the synchronous client serializes requests and validates
//! IDs. No terminal rendering, Unix socket selection, or GUI dependencies here.
use anyhow::{bail, Context, Result};
use prost::Message;
use std::io::{self, Read, Write};
use zeroize::{Zeroize, Zeroizing};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/xfer.ipc.v1.rs"));
}
pub use pb::{request::Operation as Op, response::Result as Reply};
pub const VERSION: u32 = 1;
pub const MAX_FRAME: usize = 1024 * 1024;
pub const OUTPUT_CHUNK: usize = 64 * 1024;
pub const JOB_PAGE: usize = 256;

// The envelope may contain credentials even in completion/command input. Keep
// generic request tracing safe rather than relying on callers to inspect every
// operation's nested fields before formatting it.
impl std::fmt::Debug for pb::Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("version", &self.version)
            .field("id", &self.id)
            .field("operation", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Operation([redacted])")
    }
}

impl Drop for pb::Command {
    fn drop(&mut self) {
        self.line.zeroize();
    }
}
impl Drop for pb::PasswordConnect {
    fn drop(&mut self) {
        self.target.zeroize();
        self.password.zeroize();
    }
}
impl std::fmt::Debug for pb::Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Command([redacted])")
    }
}
impl std::fmt::Debug for pb::PasswordConnect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordConnect([redacted])")
    }
}
impl std::fmt::Debug for pb::OutputChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputChunk")
            .field("next_offset", &self.next_offset)
            .field("bytes", &self.data.len())
            .field("eof", &self.eof)
            .finish()
    }
}

/// Allocate only after checking the untrusted length. EOF is valid only between
/// frames; truncated headers/bodies and malformed protobufs are fatal.
pub fn read_frame<M: Message + Default>(reader: &mut impl Read) -> io::Result<Option<M>> {
    let mut header = [0u8; 4];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid IPC frame length",
        ));
    }
    let mut body = Zeroizing::new(vec![0u8; length]);
    reader.read_exact(&mut body)?;
    // Do not include decoder diagnostics: a malformed field may contain secrets.
    M::decode(body.as_slice())
        .map(Some)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid IPC protobuf"))
}

pub fn write_frame<M: Message>(writer: &mut impl Write, message: &M) -> io::Result<()> {
    let length = message.encoded_len();
    if length == 0 || length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC message exceeds frame limit",
        ));
    }
    let mut body = Zeroizing::new(Vec::with_capacity(length));
    message
        .encode(&mut *body)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cannot encode IPC protobuf"))?;
    writer.write_all(&(length as u32).to_be_bytes())?;
    writer.write_all(&body)?;
    writer.flush()
}

#[derive(Debug)]
pub struct RemoteError {
    pub code: pb::ErrorCode,
    pub message: String,
}
impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RemoteError {}

#[derive(Debug)]
pub struct Client<S> {
    stream: S,
    next_id: u64,
    failed: bool,
}
impl<S: Read + Write> Client<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            next_id: 1,
            failed: false,
        }
    }
    pub fn transport(&self) -> &S {
        &self.stream
    }
    pub fn call(&mut self, operation: Op) -> Result<Reply> {
        if self.failed {
            bail!("IPC connection is unusable; reconnect to the daemon");
        }
        let id = self.next_id;
        self.next_id = id.checked_add(1).context("IPC request ID exhausted")?;
        let request = pb::Request {
            version: VERSION,
            id,
            operation: Some(operation),
        };
        let response = (|| -> Result<pb::Response> {
            write_frame(&mut self.stream, &request)?;
            read_frame(&mut self.stream)?
                .context("daemon disconnected; restart it if it uses an older IPC protocol")
        })();
        let response = match response {
            Ok(value) => value,
            Err(error) => {
                self.failed = true;
                return Err(error)
                    .context("daemon IPC failed; restart daemons using an older protocol");
            }
        };
        if response.version != VERSION || response.id != id || response.result.is_none() {
            self.failed = true;
            bail!("incompatible or mismatched daemon IPC response; restart the daemon");
        }
        match response.result.unwrap() {
            Reply::Error(error) => {
                let code =
                    pb::ErrorCode::try_from(error.code).unwrap_or(pb::ErrorCode::Unspecified);
                if code == pb::ErrorCode::UnsupportedVersion {
                    self.failed = true;
                }
                Err(RemoteError {
                    code,
                    message: error.message,
                }
                .into())
            }
            value => Ok(value),
        }
    }
    pub fn attach(&mut self) -> Result<()> {
        match self.call(Op::Attach(pb::Empty {}))? {
            Reply::Attached(_) => Ok(()),
            _ => self.unexpected(),
        }
    }
    pub fn probe(&mut self) -> Result<bool> {
        match self.call(Op::Probe(pb::Empty {}))? {
            Reply::Probe(value) => Ok(value.available),
            _ => self.unexpected(),
        }
    }
    pub fn detach(&mut self) -> Result<()> {
        self.ok(Op::Detach(pb::Empty {}))
    }
    pub fn ok(&mut self, op: Op) -> Result<()> {
        match self.call(op)? {
            Reply::Ok(_) => Ok(()),
            _ => self.unexpected(),
        }
    }
    pub fn command(&mut self, line: &str) -> Result<pb::CommandResult> {
        self.command_reply(Op::Command(pb::Command { line: line.into() }))
    }
    pub fn command_reply(&mut self, op: Op) -> Result<pb::CommandResult> {
        match self.call(op)? {
            Reply::Command(value) => Ok(value),
            _ => self.unexpected(),
        }
    }
    pub fn local_cwd(&mut self) -> Result<std::path::PathBuf> {
        match self.call(Op::LocalCwd(pb::Empty {}))? {
            Reply::LocalCwd(value) => {
                let path = std::path::PathBuf::from(value.path);
                if !path.is_absolute() {
                    return self.unexpected();
                }
                Ok(path)
            }
            _ => self.unexpected(),
        }
    }
    pub fn session(&mut self) -> Result<pb::Session> {
        match self.call(Op::Session(pb::Empty {}))? {
            Reply::Session(value) => Ok(value),
            _ => self.unexpected(),
        }
    }
    pub fn settings(&mut self, name: Option<&str>) -> Result<Vec<pb::Setting>> {
        match self.call(Op::Settings(pb::SettingsRequest {
            name: name.map(str::to_owned),
        }))? {
            Reply::Settings(value) => Ok(value.settings),
            _ => self.unexpected(),
        }
    }
    pub fn job(&mut self, id: u64) -> Result<pb::Job> {
        match self.call(Op::Job(pb::JobId { id }))? {
            Reply::Job(value)
                if value.id == id
                    && pb::JobState::try_from(value.state)
                        .is_ok_and(|state| state != pb::JobState::Unspecified) =>
            {
                Ok(value)
            }
            _ => self.unexpected(),
        }
    }
    pub fn output(&mut self, job_id: u64, offset: u64) -> Result<pb::OutputChunk> {
        match self.call(Op::ReadOutput(pb::OutputRequest { job_id, offset }))? {
            Reply::Output(value)
                if value.data.len() <= OUTPUT_CHUNK
                    && offset.checked_add(value.data.len() as u64) == Some(value.next_offset)
                    && (value.eof || !value.data.is_empty()) =>
            {
                Ok(value)
            }
            _ => self.unexpected(),
        }
    }
    pub fn unexpected<T>(&mut self) -> Result<T> {
        self.failed = true;
        bail!("unexpected daemon IPC response");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn request(id: u64) -> pb::Request {
        pb::Request {
            version: VERSION,
            id,
            operation: Some(Op::Session(pb::Empty {})),
        }
    }
    fn response(id: u64, result: Reply) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_frame(
            &mut bytes,
            &pb::Response {
                version: VERSION,
                id,
                result: Some(result),
            },
        )
        .unwrap();
        bytes
    }
    struct Fragmented<R>(R);
    impl<R: Read> Read for Fragmented<R> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let count = out.len().min(1);
            self.0.read(&mut out[..count])
        }
    }
    #[test]
    fn fragmented_and_coalesced_frames_preserve_message_boundaries() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request(1)).unwrap();
        write_frame(&mut bytes, &request(2)).unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            request(1).encoded_len()
        );
        let mut reader = Fragmented(Cursor::new(bytes));
        assert_eq!(
            read_frame::<pb::Request>(&mut reader).unwrap(),
            Some(request(1))
        );
        assert_eq!(
            read_frame::<pb::Request>(&mut reader).unwrap(),
            Some(request(2))
        );
        assert!(read_frame::<pb::Request>(&mut reader).unwrap().is_none());
    }
    #[test]
    fn version_one_session_frame_has_stable_tags_and_big_endian_length() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request(1)).unwrap();
        assert_eq!(bytes, [0, 0, 0, 6, 0x08, 1, 0x10, 1, 0x7a, 0]);
        assert_eq!(
            response(1, Reply::Attached(pb::Empty {})),
            [0, 0, 0, 6, 0x08, 1, 0x10, 1, 0x52, 0]
        );
    }

    #[test]
    fn invalid_lengths_are_rejected_before_reading_or_allocating_the_body() {
        for size in [0u32, MAX_FRAME as u32 + 1, u32::MAX] {
            let mut reader = Cursor::new(size.to_be_bytes());
            assert_eq!(
                read_frame::<pb::Request>(&mut reader).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(reader.position(), 4);
        }
    }
    #[test]
    fn eof_inside_header_or_body_is_not_a_clean_disconnect() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request(1)).unwrap();
        for end in 1..bytes.len() {
            assert_eq!(
                read_frame::<pb::Request>(&mut Cursor::new(&bytes[..end]))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
    }
    #[test]
    fn oversized_outgoing_messages_write_nothing() {
        let mut writer = Vec::new();
        let message = pb::OutputChunk {
            next_offset: 0,
            data: vec![1; MAX_FRAME + 1],
            eof: true,
        };
        assert!(write_frame(&mut writer, &message).is_err());
        assert!(writer.is_empty());
    }
    #[test]
    fn malformed_protobuf_errors_and_debug_never_expose_credentials_or_file_bytes() {
        let secret = "secret-credential";
        let request = pb::Request {
            version: VERSION,
            id: 1,
            operation: Some(Op::ConnectPassword(pb::PasswordConnect {
                target: secret.into(),
                password: secret.into(),
            })),
        };
        assert!(!format!("{request:?}").contains(secret));
        let request = pb::Request {
            version: VERSION,
            id: 1,
            operation: Some(Op::Command(pb::Command {
                line: secret.into(),
            })),
        };
        assert!(!format!("{request:?}").contains(secret));
        let request = pb::Request {
            version: VERSION,
            id: 1,
            operation: Some(Op::Complete(pb::CompletionRequest {
                kind: pb::CompletionKind::Bookmark as i32,
                input: secret.into(),
            })),
        };
        assert!(!format!("{request:?}").contains(secret));
        let chunk = pb::OutputChunk {
            data: secret.as_bytes().into(),
            next_offset: 0,
            eof: true,
        };
        assert!(!format!("{chunk:?}").contains(secret));
        let mut bytes = vec![0, 0, 0, 2, 0xff, 0xff];
        bytes.extend(secret.as_bytes());
        let error = read_frame::<pb::Request>(&mut Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!format!("{error:?}").contains(secret));
    }
    #[test]
    fn unknown_fields_can_be_skipped_without_changing_known_operations() {
        let mut body = request(7).encode_to_vec();
        // Unknown field 100, wire type 2, length 3: "new".
        body.extend([0xa2, 0x06, 3, b'n', b'e', b'w']);
        let mut bytes = (body.len() as u32).to_be_bytes().to_vec();
        bytes.extend(body);
        assert_eq!(
            read_frame::<pb::Request>(&mut Cursor::new(bytes)).unwrap(),
            Some(request(7))
        );
    }
    #[derive(Debug)]
    struct Scripted {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Read for Scripted {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.input.read(out)
        }
    }
    impl Write for Scripted {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn mismatched_ids_versions_and_missing_results_poison_the_connection() {
        let mut wrong_version = response(1, Reply::Ok(pb::Empty {}));
        let mut decoded = read_frame::<pb::Response>(&mut Cursor::new(&wrong_version))
            .unwrap()
            .unwrap();
        decoded.version += 1;
        wrong_version.clear();
        write_frame(&mut wrong_version, &decoded).unwrap();
        let mut missing = Vec::new();
        write_frame(
            &mut missing,
            &pb::Response {
                version: VERSION,
                id: 1,
                result: None,
            },
        )
        .unwrap();
        for bytes in [response(2, Reply::Ok(pb::Empty {})), wrong_version, missing] {
            let mut client = Client::new(Scripted {
                input: Cursor::new(bytes),
                output: Vec::new(),
            });
            assert!(client.ok(Op::Session(pb::Empty {})).is_err());
            let count = client.transport().output.len();
            assert!(client.ok(Op::Session(pb::Empty {})).is_err());
            assert_eq!(client.transport().output.len(), count);
        }
    }
    #[test]
    fn typed_remote_errors_do_not_desynchronize_subsequent_calls() {
        let mut bytes = response(
            1,
            Reply::Error(pb::Error {
                code: pb::ErrorCode::NotFound as i32,
                message: "no such job".into(),
            }),
        );
        bytes.extend(response(2, Reply::Ok(pb::Empty {})));
        let mut client = Client::new(Scripted {
            input: Cursor::new(bytes),
            output: Vec::new(),
        });
        let error = client.job(9).unwrap_err();
        assert_eq!(
            error.downcast_ref::<RemoteError>().unwrap().code,
            pb::ErrorCode::NotFound
        );
        client.ok(Op::Cancel(pb::JobId { id: 9 })).unwrap();
    }
    #[test]
    fn job_replies_must_match_the_requested_job_and_have_a_known_state() {
        for (id, state) in [(2, pb::JobState::Running as i32), (1, 0), (1, 999)] {
            let bytes = response(
                1,
                Reply::Job(pb::Job {
                    id,
                    state,
                    ..Default::default()
                }),
            );
            let mut client = Client::new(Scripted {
                input: Cursor::new(bytes),
                output: Vec::new(),
            });
            assert!(client.job(1).is_err());
            assert!(client.failed);
        }
    }

    #[test]
    fn native_binary_chunks_and_explicit_eof_round_trip_without_json() {
        let data = (0..OUTPUT_CHUNK)
            .map(|n| (n % 256) as u8)
            .collect::<Vec<_>>();
        let bytes = response(
            1,
            Reply::Output(pb::OutputChunk {
                data: data.clone(),
                next_offset: data.len() as u64,
                eof: true,
            }),
        );
        let mut client = Client::new(Scripted {
            input: Cursor::new(bytes),
            output: Vec::new(),
        });
        let chunk = client.output(1, 0).unwrap();
        assert_eq!(chunk.data, data);
        assert!(chunk.eof);
    }
}
