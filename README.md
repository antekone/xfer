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
