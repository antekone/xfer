# Daemon IPC, version 1

The authoritative schema is [`proto/xfer.proto`](../proto/xfer.proto). It is
language-neutral and has Java package `org.xfer.ipc.v1`. No GUI is included.
Rust bindings are generated into Cargo's `OUT_DIR` by `build.rs`, using `protox`
and `prost-build`; a normal Cargo build **does not require an installed protoc**.
Do not edit generated bindings or duplicate message definitions in a frontend.

`xfer::ipc::Client<S>` is a reusable synchronous client for any `Read + Write`
transport. The TUI supplies a `UnixStream`. A future GUI can use the same client
from a worker thread, or generate its own bindings from the schema. Socket
selection, spawned-child peer verification, and terminal handling are outside
this transport-independent client.

## Bootstrap and discovery

The private startup stdout pipe and atomic `.meta` discovery files retain their
small JSON endpoint record (`id`, `pid`, `socket`). This is discovery data, not
the RPC stream. The parent connects directly to the newly spawned child's
announced endpoint and verifies its kernel peer PID where supported, before
sending any RPC or credentials. A separate GUI executable will still need a
supported daemon-launch mechanism: the existing parent-executable guard only
permits the xfer client and is currently Linux-specific.

## Framing

Each Unix-socket message is:

1. An **unsigned 32-bit big-endian** payload length.
2. Exactly that many bytes of one Protobuf `Request` (client to daemon) or
   `Response` (daemon to client).

Lengths must be in `1..=1048576`. Reject invalid lengths before allocating a
body. EOF is clean only between frames; partial headers/bodies, malformed
Protobufs, and failed writes terminate the connection. Decode diagnostics must
not echo wire contents. Message buffers are zeroized by the Rust implementation.
Protobuf's standard `writeDelimitedTo`/`parseDelimitedFrom` use **varint** length
prefixes and therefore are **not** this framing; Java can use
`DataOutputStream.writeInt()` / `DataInputStream.readInt()` with explicit bounds.

No newline delimiters, magic reply strings, JSON byte arrays, gRPC, or HTTP/2
are used on the RPC connection.

## Version, IDs, and attachment

Every envelope carries `version = 1`. Every request has a nonzero, strictly
increasing ID, and its response repeats that ID. The Rust client issues
sequential IDs, permits one outstanding call, and rejects mismatched IDs,
versions, missing/unexpected result variants, and invalid output offsets.
A framing or response-contract failure makes that client unusable; do not try
to resynchronize it or automatically replay potentially mutating commands.
Semantic errors such as `NOT_FOUND` leave the connection synchronized.

The first operation is normally `attach`. The daemon rejects an incompatible
version with `UNSUPPORTED_VERSION` and closes the connection. It rejects work
before attachment with `NOT_ATTACHED`; it never evaluates command text in that
case. Exactly one attached frontend owns the current daemon session. `BUSY`
means another frontend has the lease. All exits release the lease.

Alternatively, an unattached connection can issue `probe`, receive typed
availability, and close. An attached client uses `detach` and waits for `ok`.
The existing lifecycle remains: detaching/disconnecting with active jobs keeps
the daemon alive; without active jobs the daemon shuts down. Discovery itself
is read-only, and metadata is unpublished before the owned socket directory is
removed on normal shutdown.

The old line-based RPC protocol is intentionally **not** supported alongside
version 1. Restart older daemons after upgrading. Legacy discovery records can
still be read, but do not imply wire compatibility.

## Operations and results

The schema provides typed operations for:

- Attaching, probing, detaching, claiming startup once, and recording the rc path.
- Session information and logical Local CWD (query/set).
- Completion, with an enum distinguishing bookmarks, remote paths/directories,
  and local files/directories. Results include a truncation flag.
- Executing the existing command language through the single command registry.
- Supplying an interactive connection password separately from command text.
- Reading a job or a bounded, ordered page of jobs; typed job state and
  indeterminate, determinate, or transfer progress.
- Reading binary output, cancelling a job, and acknowledging completed output.
- Reading typed settings; secret settings expose only whether a value is set.

A command result has human-readable `lines` plus an optional typed action:
`job_started`, `password_required`, or `edit_bookmarks`. Frontends must not
search display text for control tokens. Semantic RPC failures have an error
code and display message. Arbitrary user command output remains presentation
text; it is not a structured file-browser API.

Output uses native Protobuf `bytes`, at most 65536 bytes per chunk. The next
offset must equal the requested offset plus the returned byte count. `eof`
means the current stored end, not necessarily that a running job has finished.
An empty chunk must have `eof = true`. For stable full output, wait for a terminal
job state as the TUI does: retries can reset a running text spool, invalidating
an earlier tailing offset. Files are acknowledged only after a job
is no longer running; acknowledgement removes its spool and job entry.

Job pages are ordered by ID, use an exclusive `after_id` cursor, and contain at
most 256 jobs (`limit = 0` requests the default maximum). Pages may be smaller to fit the
frame cap. `next_after_id` is
present when more jobs remain. This is a live view, not an immutable snapshot:
new jobs may appear and acknowledged jobs may disappear between pages.
Responses over the framing cap yield `RESPONSE_TOO_LARGE`; narrow the query or
request a smaller page.

## Credentials and diagnostics

Protobuf is serialization, not encryption or authentication. The current
transport is local, protected by per-user directory/socket permissions. Do not
expose this API on an unauthenticated TCP listener; a future network transport
would need its own authentication, authorization, and confidentiality design.

Treat command requests, password requests, and output data as sensitive. Never
log raw frames. Rust request command/password fields are zeroized on drop;
serialized frame buffers are also zeroized. Their Debug implementations are
redacted; raw output Debug prints byte counts only. Stored passwords remain
intentional plaintext configuration, but settings RPCs never return them.

Sensitive schema fields carry `debug_redact`. Do **not** assume every generated
language's `toString()` or text printer respects this annotation. In particular,
the tested Java Protobuf runtime's ordinary `toString()` can expose fields;
`com.google.protobuf.DebugFormat.singleLine()` honors the annotations. Avoid
logging these messages at all, even with a redacting formatter. Immutable Java
strings and third-party library copies are not guaranteed to be zeroized.

## Generating another client's bindings

For Java, with a matching Protobuf Java runtime and a recent `protoc` (22 or
newer, for the debug-redaction field option):

```sh
mkdir -p generated
protoc --proto_path=proto --java_out=generated proto/xfer.proto
```

A generated Java client has been exercised against the real daemon for
attachment, session state, setting redaction, job state/progress, output bytes,
errors, acknowledgement, and detachment. This is protocol validation, not a GUI.

## Evolution and future GUI work

Never reuse removed field or enum numbers; reserve them when removing a field.
Use a new enum/oneof alternative rather than encoding control instructions in
text. Additive optional metadata can remain compatible within version 1.
Changes to framing, required semantics, or interactive actions that older
clients cannot safely ignore require version negotiation or a version bump.

Version 1 is deliberately request/response with job polling, preserving the
functional TUI. It does not send unsolicited events or support multiple attached
frontends. Push subscriptions, capabilities, structured directory-entry pages,
and simultaneous GUI/TUI ownership are future API extensions—not implemented
GUI features. Do not send unsolicited frames to version-1 clients: their client
expects the response ID for the outstanding request.
