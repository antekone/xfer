# xfer

A runnable Rust scaffold for an lftp-like transfer client. It implements the
runtime shape, not network protocol implementations yet:

- lftp-style inline prompt in the normal terminal buffer; only pager mode uses the alternate screen;
- Tokio jobs with foreground/background state, progress updates and cooperative
  `Ctrl-C` cancellation;
- `Ctrl-Z` backgrounds the foreground job; `jobs` and `fg [id]` manage jobs;
- pager mode (`help`, `q`/Escape, arrows/Space) and the `ui:auto-pager` setting;
- settings registered by core and SFTP/S3/SMB backend stubs before configuration
  is read and backend initialization occurs.

Run it with:

```sh
cargo run
```

Try `help`, `get example`, `mirror remote-dir`, `jobs`, `fg`, and `set
ui:auto-pager false`.

## Configuration

`xfer.conf` in the working directory is read first; `XFER_CONFIG`, if set, is
read afterward and overrides it. The format is `name: value`, including
namespaced keys:

```text
ssl:skip-validation: false
ui:auto-pager: true
sftp:connect-timeout: 30
s3:region: us-east-1
smb:max-credits: 32
```

The transfer commands currently run simulated jobs. Replace `start_demo` with
protocol-specific job factories once SFTP, S3, and SMB adapters exist.

## Command history

`ui:history-file` defaults to `$XDG_STATE_HOME/xfer/history` (or
`~/.local/state/xfer/history`). Set it to `session` to retain history only
while the process runs, or `/dev/null` to disable history entirely. Set it in
`xfer.conf` before startup, for example:

```text
ui:history-file: session
```


## Daemon process

The same `xfer` executable runs the per-session daemon with `xfer --daemon`.
Normal `xfer` startup spawns that mode automatically, then attaches through its
private Unix socket. There is no separate `xferd` binary.

## S3 connections (first backend)

`connect s3://bucket` selects the backend by its registered `s3` protocol token.
The AWS Rust SDK uses its default credential chain (environment, shared AWS
config, workload/instance credentials). `disconnect` releases the active logical
connection. Transfers are not implemented yet. Connection changes run as
cancellable daemon jobs; failed connects preserve the previous connection.

`connect` probes bucket access using `HeadBucket`. For an endpoint-only bookmark,
`ListBuckets` validates account access instead. This requires the corresponding
permissions; specify `bucket` to use a bucket-level probe. S3 has no persistent
login/session to close: disconnect drops the SDK client.

### Bookmarks

Use TOML, not a custom DSL. Default file:
`$XDG_CONFIG_HOME/xfer/bookmarks.toml` or `~/.config/xfer/bookmarks.toml`.
Override with `bookmarks:file` in `xfer.conf`/`XFER_CONFIG` or `set`.

```toml
# Local MinIO account. This comment is preserved.
[minio]
type = 's3'
url = 'https://minio-api.cleaner.hq.eset.com'
accessKey = '...'
secretKey = '...'
api = 's3v4'
region = 'us-east-1'
# bucket = 'example' # Optional: probe this bucket instead of ListBuckets.
```

Inline tables are also valid:

```toml
archive = { type = 's3', url = 's3://archive-bucket' }
```

Run `bookmark edit` to edit with `$VISUAL`, `$EDITOR`, or `vi`, then
`connect minio`. Files are re-read for each connection. xfer never rewrites
bookmarks, preserving comments and formatting; newly created files are mode
0600. Protect existing files yourself and prefer the AWS credential chain when
possible. Credentials are not included in command output or parse diagnostics.

`s3:region` defaults to `us-east-1`; `s3:endpoint` optionally overrides the endpoint
for direct bucket URLs. Custom endpoints use path-style addressing. TLS
certificate validation is enabled by default. `set ssl:skip-validation true` opts
into disabling certificate trust/hostname validation for subsequent S3 connections
(unsafe on untrusted networks). Only SigV4 is supported.

### Listing buckets and objects

```text
connect minio
ls
```

When the bookmark has no `bucket`, `ls` lists the account's available buckets
(requires `ListAllMyBuckets`). With a selected bucket, `ls` lists its top-level
objects and directory prefixes; `ls photos/` lists under that prefix (requires
`ListBucket`). Results are paginated by the backend and written to a spool file.
Long output opens the pager when `ui:auto-pager` is enabled; use Space/PgDn,
PgUp, and `q`. Once displayed or dismissed, output and the finished job are
removed. Ctrl-Z during listing leaves the job in the background for later `fg`.

## Startup commands

After the first client attachment to a daemon, xfer reads `~/.xferrc` and
executes each line through the same command path as interactive input, in order:

```text
# Executed before the first prompt
set s3:region us-east-1
set ui:history-file session
# connect minio
```

Blank lines and lines starting with `#` (after optional whitespace) are ignored.
A missing file is fine. Foreground jobs complete before the next command runs.
`exit`/`detach` stops startup processing. Startup commands are not added to command
history. The file runs once per daemon session, not on subsequent `xfer attach`;
it is not executed by `xfer --daemon` without a client.

### SSH tunnels / TCP destination overrides

Keep the real endpoint URL so TLS SNI, certificate verification, HTTP Host, and
SigV4 signing retain the server's identity. Override only the TCP destination:

```toml
[minio]
type = 's3'
url = 'https://minio-api.cleaner.hq.eset.com'
override_host = '127.0.0.1'
override_port = 9999
accessKey = '...'
secretKey = '...'
api = 's3v4'
# bucket = 'example'
```

For `ssh -R 9999:minio-api.cleaner.hq.eset.com:443 invoker`, xfer on invoker
connects to `127.0.0.1:9999` but still identifies the original HTTPS endpoint.
Certificate validation remains on unless explicitly disabled. No `/etc/hosts`
changes are needed. Without `override_port`, the URL's port is used (443 for
HTTPS); e.g. a URL ending in `:9999` needs only `override_host`. These overrides
currently apply to S3 bookmarks and affect neither the URL path nor TLS name.

## Retry policy

`connect` and `ls` retry failed backend operations indefinitely. Delays are
3, 6, 12, 24, then 30 seconds between attempts, staying at 30 seconds until the
operation succeeds or is cancelled. Foreground progress includes the last error
and the next delay. Ctrl-C cancels either an active request or a backoff wait;
background jobs can be cancelled with `cancel <id>`. SDK-internal retries are
disabled, avoiding nested backoff policies.

Each failed listing attempt's partial output is discarded before retrying, so
restarting a paginated listing does not duplicate entries. Connection attempts
keep their settings snapshot and preserve an existing connection on failure.
Syntax errors/unknown bookmarks are still reported immediately before a job
starts. File transfers are not implemented yet; future write operations will
need idempotency/resume handling rather than blindly replaying writes.

### Listing records and display

Backends emit typed `Content` records (`Bucket`, `Directory`, `File`, `Comment`)
through a `ListingSink`, one entry at a time. `src/listing.rs` defines these types
and the common renderer; backends do not format terminal output. The daemon's
spool adapter invokes that renderer. This keeps paginated listings bounded and
allows future completion/picker sinks to consume metadata without parsing text.

Example output (fixed-width date and size columns):

```text
[2025-04-16 10:11:00 UTC]          -  ci-production/
[                      -]          -  src/
[2026-10-16 13:22:00 UTC]    4.0 KiB  notes.txt
```

S3 object dates are `LastModified` values from `ListObjectsV2`; bucket dates are
creation dates from `ListBuckets`, not modification times. Virtual directory
prefixes have no timestamp, and neither buckets nor prefixes have an aggregate
size in the listing API: these fields display `-` rather than fabricated dates
or zero sizes. All dates are UTC; file sizes use binary units. Permissions and
ownership are omitted since ordinary S3 listing metadata does not contain them.
Names preserve Unicode, while terminal control characters are escaped.

### Working directory

The daemon-owned backend connection holds its working directory. It survives
client detach/reattach and resets on a new connection. For S3, `/` is the account
root (buckets); `/bucket/photos/` is a bucket/prefix location:

```text
open minio
cd archive
cd photos
pwd                 # /archive/photos/
ls                  # names relative to photos/: a.jpg, sub/, ...
ls ../documents     # does not change pwd
cd ..
cd /other-bucket
cd                  # return to the connection's initial directory
```

`cd .`, `cd ..`, absolute paths, and trailing `/` are supported. A bucket bookmark
starts at `/its-bucket/`; plain `cd` returns there, while `cd /` selects the account
root. `cd /` really clears the active bucket and subsequent `ls` uses account-level
`ListBuckets`, not the original bucket's objects. The bookmark's initial bucket
is retained only as the home destination for bare `cd`, not as a hidden active
bucket. `cd` validates bucket/prefix existence before changing state; errors or
cancellation leave the previous directory intact. A virtual prefix must contain
an object (including a directory marker) to exist. Missing directories fail
immediately; network errors retain the retry/backoff policy. Relative paths are
resolved once per job so retries do not drift if the working directory changes.
As with shell paths, `.`/`..` and repeated `/` are normalized; unusual S3 keys
using those literal path components are not navigable with `cd`.

Explicit `ls` always fetches fresh content and refreshes the typed directory
cache used by completion; manual listings are never silently served stale data.


### Cached directory completion

`cd <TAB>` completes only directory and bucket names, plus `../`. Files and
comments are excluded. Prefixes and nested/absolute paths work (`cd pho<TAB>`,
`cd ../ar<TAB>`, `cd /archive/pho<TAB>`); names containing spaces are inserted
with shell quoting. Commands now accept quoted arguments.

The cache belongs to the backend connection, survives client reattachment, and
is discarded on close/reconnect. A successful manual `ls` refreshes it. On a cold
or expired directory, TAB fetches a bounded listing; that lookup has a 3-second
timeout and does not start a retrying job or change `pwd`. Warm completion uses
memory only. Directory changes still validate against the server, not the cache.

Defaults (applied to new connections):

```text
cache:ttl: 30
cache:max-bytes: 4194304
```

Set `cache:ttl` or `cache:max-bytes` to `0` to disable retention. The cache uses
LRU eviction, at most 32 directories, 2048 entries/1 MiB per directory, and the
configured total estimated-memory budget. Large completion listings are explicitly
marked as limited; TAB displays at most 20 choices at once. Manual `ls` remains
unlimited and streams every result to disk even if the cached snapshot is partial.
Failed refreshes invalidate their old snapshot rather than caching partial errors.

Prompt editing supports Ctrl-W (erase trailing whitespace and the preceding
whitespace-delimited word). TAB extends to the longest shared prefix of all
matches, for commands, setting names, and `cd` paths. For example, `cd bui<TAB>`
with `builddylib-ci/` and `builddylib-remote-ci/` becomes `cd builddylib-`.
Common prefixes are computed before shell quoting, on Unicode character
boundaries. A partial/limited cached listing does not auto-extend the prefix,
since it may be missing other matches.

### Prompt and daemon status

The prompt shows the committed backend working directory, not a process ID:

```text
xfer> open minio
xfer[s3:/]> cd archive
xfer[s3:/archive/]> cd photos
xfer[s3:/archive/photos/]> status
```

`status` displays the connected daemon's PID, backend, CWD, and running-job count.
The prompt refreshes before each new command line after connect/cd/close or
reattach. It reads a lightweight session snapshot without creating a job or
waiting for a background operation to release the backend connection lock.

### `ls` path completion

`ls <TAB>` uses the same typed cache as `cd`, but includes files as well as
buckets, directories, and `../`. Directories/buckets get a trailing `/`; files
do not. Common-prefix completion, nested/absolute paths, quoting, and cache
expiry/refetch work for both commands. `cd` continues to exclude files.

```text
ls READ<TAB>           # README.txt
ls photos/im<TAB>      # photos/image.jpg
ls README.txt          # show metadata for this exact file
ls photos/            # list directory contents, without changing cwd
```

S3 file metadata is fetched with `ListObjectsV2`, so listing an individual file
does not require `GetObject` permission. If both a file `foo` and a virtual
prefix `foo/` exist, `ls foo` selects the file and `ls foo/` lists the directory.
Single-file results are not stored as directory cache entries.

### Typeahead during jobs

While a foreground job runs or waits to retry, printable keys are buffered for
the next command line instead of discarded. Backspace and Ctrl-W edit this
pending text. When the job finishes, is cancelled with Ctrl-C, or is backgrounded
with Ctrl-Z, the next prompt is prefilled with it. Startup foreground jobs share
the same buffer. Buffered text is added to history only when submitted normally.

Enter during a job does **not** queue execution: review the prefilled line and
press Enter at the prompt. Other navigation/control keys are not stored; Ctrl-C
and Ctrl-Z retain their immediate job-control meanings. Typeahead is bounded to
16 KiB and does not echo over job progress. Pager and external-editor keys retain
their usual behavior rather than becoming typeahead.
