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
- Fresh non-recursive local/remote directory snapshot jobs, bounded entry pages,
  regexp-filtered views, and explicit close/cancellation (used by Ctrl-S).
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

## Directory snapshots

`start_directory` accepts a `DirectorySource` (`LOCAL` or `REMOTE`) and a
`DirectoryFilter` (`ALL`, `DIRECTORIES`, or `REGULAR_FILES`). It returns
`directory_started` containing a job ID. Poll the normal `job` operation until
completion before calling `read_directory`. Failures/cancellation are reported
through job state/progress, not partial directory pages. These scans do not
retry automatically and never change either working directory.

A snapshot contains immediate children of the current logical directory,
including dotfiles, initially sorted by name A–Z. It is immutable once
complete and is collected into private disk-backed data/index files (directory
0700, files 0600), not an unbounded in-memory array or the completion cache.
Remote paths are resolved once while holding the backend connection lease.
Local `REGULAR_FILES` excludes final symlinks and special files; directory
symlinks can be included for local directory navigation.

`read_directory` takes `job_id`, an entry-index `offset` (initially zero), and
`limit` (zero defaults to 256; larger values are clamped). Responses contain at
most 256 entries and may be shorter to fit the frame byte limit. Each entry has
`name`, `directory`, `regular_file`, `index`, and optional `size` fields.
`size` is a reported regular-file byte count; absence means unavailable, not
zero. Directories and non-regular local entries do not have recursive sizes. `index` is the stable
original snapshot index, not the entry's position in a filtered view; preserve
selection by this ID when changing filters. `next_offset` equals offset plus
entry count; `eof` is true exactly at `total`. An empty non-EOF page is invalid.
The response also includes the resolved directory `path` and a `skipped` count
for unsafe/non-UTF-8/unrepresentable names. Names are basenames without slashes,
`.`/`..`, or control characters, and are at most 64 KiB. The TUI adds directory
slashes and quotes names itself; clients must never parse rendered `ls` output.

`filter_directory` takes a completed snapshot's `job_id` and a `regexp` and
plus optional `sort` (`NAME_ASC`, `NAME_DESC`, `SIZE_DESC`, `SIZE_ASC`; default
`NAME_ASC`) and returns another `directory_started` job. Poll it and read pages just like the
original snapshot. It streams a private disk-backed view from the original
snapshot, preserving entry indices, sizes, path, and skipped count. Name sorts
are case-sensitive; size sorts put unavailable sizes last in either direction,
with name A–Z tie-breaking. Equal names are tied by type and original index.
The stable indices need not be monotonic in a sorted page, but are unique. `total` and
page offsets refer to the filtered view, while entry indices still identify
original entries. Filtering must not use a partial completion listing or fetch
the backend again. The TUI always filters the original snapshot, not its last
filtered subset, so replacing a filter can reveal previously excluded entries.

Sorting uses private numbered merge runs with at most 2048 entries/about 1 MiB
per in-memory run, a constant number of open files, and cooperative yields during
collection/merge/output. No full-directory vector or path list is retained;
errors/cancellation remove partial runs through snapshot ownership. Default
snapshot indices are assigned after initial name sorting and retained through
all later filtered/sorted views.

Patterns use Rust `regex` syntax, Unicode enabled, case-sensitive by default.
Empty patterns and `.*` match everything. Patterns over 4096 UTF-8 bytes, invalid
syntax, and compiled expressions exceeding the 1 MiB compilation budget return
`INVALID_REQUEST` without poisoning the connection. Regex DFA memory is also
bounded to 1 MiB. Superseded filter jobs can be closed immediately; previews and
applied views are independently owned jobs and must each be closed. Parent
snapshot close does not invalidate already built views.

`close_directory` cancels and removes the snapshot job even if it is still
scanning, releasing private files. It cannot close unrelated jobs. Normal
acknowledgement also removes a completed snapshot job. Detach, EOF, and lease
unwind automatically cancel/discard all snapshot jobs owned by that attachment,
without cancelling normal background transfers. Frontends should still close
snapshots promptly rather than waiting for disconnect.

These operations are additive in version 1. Older daemons reject them as an
unsupported operation; existing operations and clients keep their contracts.
A full file-browser metadata/action API and change subscriptions remain future
work; these minimal name/type pages do not implement a GUI.

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
frontends. Push subscriptions, capabilities, richer file-browser metadata/actions,
and simultaneous GUI/TUI ownership remain future API extensions—not implemented
GUI features. Do not send unsolicited frames to version-1 clients: their client
expects the response ID for the outstanding request.

`Session.prompt_paths` contains ordered, non-overlapping UTF-8 byte ranges in
`Session.prompt` for path substitutions. Terminal clients can shorten only
those ranges while preserving the surrounding template. Older peers may omit
these additive fields; clients must validate boundaries before slicing.
