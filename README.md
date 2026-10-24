# xfer

A Rust prototype for an lftp-like transfer client, with S3/SFTP/SMB/WebDAV backends and a
client/daemon runtime:

- lftp-style inline prompt in the normal terminal buffer; only pager mode uses the alternate screen;
- Tokio jobs with foreground/background state, progress updates and cooperative
  `Ctrl-C` cancellation;
- `Ctrl-Z` backgrounds the foreground job; `jobs` and `fg [id]` manage jobs;
- pager mode with vi-style navigation, `$VISUAL` handoff, and the `ui:auto-pager` setting;
- typed settings and backend interfaces, with S3/SFTP/SMB/WebDAV listing/download support
  and SFTP/SMB/WebDAV uploads, plus file renaming on all four backends.

Run it with:

```sh
cargo run
```

Try `help`, `open <bookmark>`, `ls`, `get <filename>`, `jobs`, and `status`.

## Configuration

`xfer.conf` in the per-user application config directory is read first;
`XFER_CONFIG`, if set, is read afterward and overrides it. Missing files are
ignored; `status` marks them as not loaded. The working directory is not
searched for configuration.

Directory lookup is centralized in `src/paths.rs` using the cross-platform
`directories` crate. Default locations are:

| Platform | Configuration | Cache / job spools | State / history |
|---|---|---|---|
| Linux | `~/.config/xfer` | `~/.cache/xfer` | `~/.local/state/xfer` |
| macOS | `~/Library/Application Support/xfer` | `~/Library/Caches/xfer` | `~/Library/Application Support/xfer` |
| Windows | `%APPDATA%\xfer\config` | `%LOCALAPPDATA%\xfer\cache` | `%LOCALAPPDATA%\xfer\data` |

Absolute `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, and `XDG_STATE_HOME` values override
these roots on any platform, with `xfer` appended. Empty or relative XDG values
are ignored. On Linux, unset XDG variables use the defaults above; an unset or
empty `HOME` falls back to the OS account database. Windows uses Known Folders,
not the presence of Unix environment variables. Home-relative paths (`~`, `lcd`,
and `~/.xferrc`) use the same native home-directory lookup.

Daemon sockets/discovery files use `$XDG_RUNTIME_DIR/xfer/instances` when that
variable is absolute, otherwise `<state-directory>/instances`. Newly created
runtime directories are private (0700 on Unix); no paths fall back to the process
working directory. Raw `cat` contents still use private temporary spools.
This is portable directory discovery, not a complete Windows port: IPC, local
shell/process handling, and other parts still contain Unix/Linux-specific code.

The configuration format is `name: value`, including namespaced keys:

```text
ssl:skip-validation: false
ui:auto-pager: true
sftp:connect-timeout: 30
sftp:max-requests: 64
s3:region: us-east-1
smb:max-credits: 32
```

S3, SFTP, SMB, and WebDAV support connect, navigation, listing and downloads.
SFTP, SMB, and WebDAV also support uploads with `put`; all four backends support
file renaming with `mv`. S3 uploads and mirroring are not implemented yet.

## Renaming remote files

```text
mv 'old name.txt' 'new name.txt'
mv file.txt subdirectory/file.txt
mv -f replacement.txt existing.txt
mv -- -old-name -new-name
```

`mv [-f] [--] <source> <destination>` renames **one regular file to an exact
filename**. Both operands are relative to the remote CWD unless absolute; the
CWD is never changed. There is no wildcard expansion, multiple-source operation,
recursive directory/prefix move, or implicit basename insertion for a destination
directory. Specify `subdir/name`, not `subdir/`. Final source symlinks/reparse
points are refused where the backend exposes that information. Use quotes for
spaces and `--` before names starting with `-`. TAB completion works for both
operands, and completion caches are cleared on success, failure, and cancellation.

Existing destinations are protected by default. `-f`/`--force` or a frozen
`xfer:clobber: true` permits replacing a file. Moving a file to its own canonical
path succeeds without changing it (after checking it exists). Moves run as normal
cancellable/backgroundable jobs, **without automatic retries**: a disconnected or
cancelled request may already have completed on the server. Inspect both paths
before repeating an ambiguous operation. No delete-then-rename overwrite fallback
is used.

- **SFTP:** standard v3 RENAME for no-replace behavior, even when the server also
  advertises POSIX rename. Overwrites require `posix-rename@openssh.com`; the
  extension is never used for a protected move. Without it, `-f` can still move
  to a free name but never replaces an existing one. Cross-filesystem moves may fail.
- **SMB:** native rename with the server's replace-if-exists option, for named and
  anonymous sessions. Both files must be in the same share; cross-share copies
  are not emulated. Case-only renames work on case-insensitive servers.
- **WebDAV:** same-origin MOVE with an encoded absolute Destination and explicit
  Overwrite header; no redirect replay, upload, or DELETE fallback. Source ETag
  conditions are sent when available (mtime fallback on protected moves).
  Replacing an existing file also
  requires a strong destination ETag and enforced tagged WebDAV If conditions.
  A non-mutating HEAD with an intentionally false condition checks support first;
  servers that ignore these conditions are refused for existing-file replacement
  (including WsgiDAV 4.3.5). This avoids blindly deleting a substituted collection.
  Source conditions on replacement moves are resource-tagged too, avoiding
  servers that incorrectly apply HTTP If-Match to both resources. Missing
  destinations still use Overwrite: F even with `-f`. HTTP 207 is not treated as
  complete success. WebDAV locks requiring tokens are not supported.
- **S3:** server-side CopyObject (up to **5 GiB**) followed by DeleteObject, within
  a bucket or between buckets at the configured endpoint. It requires source
  read/copy, destination write, source delete, and verification HEAD permissions.
  Copy uses the source ETag and pins a version ID when available; protected copies
  require destination If-None-Match: *. After copying, destination size/ETag and
  source size/ETag/version/mtime are checked before conditional source deletion.
  Unsupported conditional requests fail rather than falling back unconditionally.
  A failed copy never authorizes source deletion, including HTTP 200 responses
  carrying embedded S3 errors. The client never rolls back by deleting a destination.

**S3 moves are not atomic or transactional.** Cancellation/failure can leave both
objects; a lost delete acknowledgement can leave only the destination. There is
no atomic condition covering both objects, and replacements with identical ETags
cannot always be distinguished, so avoid concurrent writers during a move. In a
versioned source bucket, deletion creates the usual delete marker rather than
removing an old version and exposing previous content. Standard CopyObject
metadata/tag copying applies; source storage class and website-redirect metadata
are retained, but ACLs/ownership and encryption may differ under
the destination bucket's rules. Buckets, slash-marker/prefix moves, and multipart
copies of larger objects are not implemented.

## WebDAV

`src/webdav.rs` implements native async HTTP/WebDAV, not HTML directory-index
scraping. HTTP(S) and DAV(S) URLs select this backend:

```text
open https://live.sysinternals.com/
ls
```

Anonymous HTTPS browsing of this public endpoint has been verified with the real
terminal client. `http://`, `dav://`, and `webdav://` use plain HTTP;
`https://`, `davs://`, and `webdavs://` use TLS. A plain Apache/nginx HTML index
without `PROPFIND` support is not sufficient.

For authenticated access, use a bookmark:

```toml
[files]
type = 'webdav'
url = 'https://dav.example.com/files/'
user = 'alice'
password = 'your password'
# Or omit password and use password_interactive = true.
```

HTTP Basic authentication and anonymous access are supported; Digest, NTLM,
OAuth/Bearer, and client certificates are not implemented. Password prompts use
the existing private IPC flow. Interactive passwords override stored bookmark
passwords, which override URL credentials and then `webdav:user`/`webdav:password`
defaults. Explicit empty values override defaults; an empty user means anonymous
and sends no inherited password. URL credentials are percent-decoded, redacted
from job labels, and excluded from history. Prefer bookmarks: inline passwords
are visible while typed. Stored passwords are plaintext; protect credential
files with mode 0600. HTTP header/TLS buffer copies are not guaranteed to be wiped
when released.

TLS certificate verification is enabled by default; the existing, deliberately
unsafe `ssl:skip-validation` switch is honored per connection. Basic credentials
are refused over plain HTTP unless `webdav:allow-insecure-auth` is explicitly
set to true, even on loopback. Redirects may only canonicalize a trailing slash
at the same origin; cross-origin, scheme-changing, and arbitrary path redirects
are refused, and mutations are never replayed through redirects.

`ls` and completion use namespace-aware `PROPFIND` with `Depth: 0`/`1`,
`get`/`cat` use streaming `GET`, `put` uses streaming `PUT`, and `mkdir [-p]` uses
`MKCOL`. `pwd` and navigation use decoded UTF-8 paths; `/` is the HTTP origin's
root, while bare `cd` returns to the URL's initial collection. Parent navigation
may fail if the origin root is not itself WebDAV-enabled. Server-provided sizes,
UTC modification dates, and collection types are used without fabricated POSIX
metadata. Foreign/deeper-than-requested hrefs, encoded path separators, DTDs,
malformed XML, and oversized XML tokens/resources are rejected. Listings stream
without a resource-count cap; XML nesting is limited to 32, tokens/resources to
1 MiB, and individual metadata fields to 64 KiB.

Transfers use bounded 64 KiB chunks. Downloads check size and validators before
and after reading, use strong ETag `If-Match` (or modification-time
`If-Unmodified-Since`), and validate exact `206`/`Content-Range` responses on
resume. Ignored ranges, changed metadata, compressed bodies, and oversized
responses cannot silently corrupt an appended file. Resume without a strong
ETag or modification date is refused; date-only checks have server timestamp
precision limits and are not atomic snapshots or content hashes. Servers must
honor HTTP preconditions. Uploads use `If-None-Match: *` unless clobber is enabled;
interrupted uploads may leave a remote file and restart rather than resume.
No WebDAV locking, atomic temporary-upload/rename, `MOVE`, or `DELETE` commands
are exposed. Cancellation drops active request/body futures; existing job
backoff reconnects HTTP transport as needed with frozen connection options.
HTTP authentication/access/missing-path/unsupported-method errors stop retries;
timeouts, throttling, and transient service failures remain retryable.

WebDAV settings (reconnect to apply):

```text
webdav:user:
# Optional: webdav:password: your password
webdav:connect-timeout: 10
webdav:operation-timeout: 30
webdav:allow-insecure-auth: false
```

Timeouts are validated to 1..300 seconds. `tests/webdav.rs` exercises real local
HTTP exchanges without external services, including credentials, TLS rejection,
range safety, disconnect/resume, cancellation, mutations, and daemon workflows.
A separate manual test against WsgiDAV and a real PTY covered stored/prompted
login, resumable binary downloads, Unicode uploads, completion, and `mkdir`.

## SMB / CIFS

`src/smb.rs` uses the pure-Rust `smb2` crate's native Tokio APIs. No CIFS mount,
root privileges, external `smbclient`, or dedicated blocking SMB thread is needed.
SMB1 is not supported. xfer currently exposes NTLM/domain-password or explicit
guest access, not the library's separate Kerberos API.

```text
open smb://nas/share
open smb://nas:1445/share/subdirectory
```

An empty effective user requests a **true anonymous / NULL session**, never the
local OS username or the named `Guest` account. This works with public Samba shares
using `map to guest = Never`; no failed-login-to-guest mapping is needed. The
adapter uses the library's public connection/tree APIs for anonymous login because
`smb2` 0.31's normal empty-credential NTLMv2 login is not a NULL session.

For a public share such as `//127.0.0.1/vm`, use:

```toml
[vm]
type = 'smb'
url = 'smb://127.0.0.1/vm'
user = ''
domain = ''
password = ''
```

Then `open vm`, without a prompt. `user = ''` is significant, not an unset value:
it forces anonymous access and discards inherited domain/password credentials.
With the default empty `smb:user`, `open smb://127.0.0.1/vm` is anonymous too.
An explicit empty URL user (`open smb://@127.0.0.1/vm`) overrides a named global
user. The named `Guest` account and passwordless logins as another username remain
separate logins; they are not substituted for anonymous access.

To store a noninteractive Samba-account password instead:

```toml
[vm-login]
type = 'smb'
url = 'smb://127.0.0.1/vm'
user = 'antek'
domain = ''
password = 'your Samba password'
```

A Samba account password is separate from the Linux account password. Protect
`bookmarks.toml` and any credential-bearing `xfer.conf` with mode 0600; stored
passwords are plaintext on disk, not encrypted. `password` is also supported in
SFTP bookmarks. It is never included in bookmark listings.

For a client-side password prompt, use `password_interactive = true` instead.
That flag deliberately forces a prompt even if a stored password is present;
the answer travels through private IPC, not argv/environment/history. Explicit
SMB bookmark fields override URL components, which override `smb:user`,
`smb:domain`, and `smb:password` defaults. The interactive password takes precedence
over every stored source. Explicit empty values override rather than fall through.

SMB URL shortcuts are also accepted:

```text
open 'smb://WORKGROUP;antek:password@nas/Documents'
```

`DOMAIN%5Cuser` is an alternative to `DOMAIN;user`; percent-encode reserved
characters in URL passwords (`%40` for `@`, `%2F` for `/`, etc.). Prefer bookmarks:
a password typed in a URL or `set smb:password ...` is still visible on the input
line. Such commands are excluded from both persistent and up/down history, job
labels redact URL userinfo, and `show`/`set` display passwords as `[redacted]`.
`set` changes in-memory defaults; use a configuration file for persistence.

Remote paths use `/share/path/`, with `/` representing the server's disk-share
namespace. `open smb://nas` / `cd /` enumerate shares when the server permits it;
connecting directly to a share does not require share-enumeration permission.
Use forward slashes, not Windows UNC backslashes: the library treats a backslash
inside a filename as a literal character. `cd` without arguments returns to the
directory selected by the connection URL. `mkdir -p` creates directories, never
shares. `ls`, completion, `get [-c]`, `cat`, `put`, and `du` use the existing job,
cache, sorting, and transfer interfaces. Listing supplies exact sizes and UTC
mtime, not fabricated POSIX permissions, UID/GID, or owner names. Descendant
reparse-point directories are not traversed by `du` or offered as directories by
completion, avoiding junction/symlink cycles.

Directory listings are read one server batch at a time. Transfers use bounded
64 KiB chunks and a capped in-flight window, rather than buffering whole files.
Cancelled, timed-out, or broken sessions are closed and replaced lazily. Existing
job backoff reconnects with frozen settings and in-memory credentials; downloads
resume at the last written byte after checking size and server modification/change
timestamps (100 ns wire precision). CreationTime is excluded because Samba may
report different birth-time estimates across opens. These metadata checks
are not atomic snapshots or cryptographic content verification. Authentication,
security, permission, and missing-path errors do not retry indefinitely. Uploads
use exclusive creation unless clobber is enabled, and flush before success;
interrupted uploads may leave a partial remote file and are not resumed in place.

SMB settings (reconnect to apply):

```text
smb:user:
smb:domain:
# Optional: smb:password: your Samba password
smb:connect-timeout: 10
smb:operation-timeout: 30
smb:max-credits: 32
smb:dfs: false
smb:require-encryption: false
```

`smb:max-credits` bounds in-flight 64 KiB transfer chunks (1..128), not the server's
negotiated credit grant. DFS is disabled by default because referrals can forward
credentials to other servers; enable it only for a trusted namespace. Anonymous
sessions currently do not support DFS. Named SMB
sessions use the library's signing verification. Encryption is not guaranteed by
default. `smb:require-encryption` requires an authenticated SMB3 encrypted session,
including the tree connect, and fails closed without a plaintext fallback. It
cannot currently be combined with DFS, since referral encryption cannot be
enforced uniformly by this adapter.

**Encryption interoperability limitation:** with `smb2` 0.31 and the tested Samba
4.24 server, no encryption cipher was negotiated, so encryption-required access
was refused. Signed authenticated and guest transfers were tested successfully;
do not assume encrypted-share compatibility until your server has been verified.

Connection-specific credentials are retained for logical reconnects and released
on close/replacement; configured global defaults remain until replaced or daemon
exit. xfer's own password handle is zeroizing, but `smb2` also retains ordinary password
and session-key allocations internally; their deallocation is not a guaranteed
secure memory wipe. xfer does not copy credentials into logs, job spools, or
history; explicitly stored passwords remain in their configured source files.

SMB integration tests use an isolated Samba instance on an ephemeral loopback
port with private passdb/state directories. They require `smbd` and `smbpasswd`,
and either root or Linux user namespaces (`unshare --map-auto --map-root-user`
plus `setpriv` and subordinate UID/GID mappings). Tests skip when unavailable and
never reconfigure the system Samba server or accounts.

## Creating remote directories

`mkdir [-p] [--] <directories...>` creates remote directories on SFTP/SMB or directory
markers on S3:

```text
mkdir downloads
mkdir -p multiple/subdirs/at/once
mkdir -p 'photo album/originals' archive/2026
mkdir -- -directory
```

Paths are relative to remote `pwd`, or absolute when starting with `/`; neither
remote CWD nor Local CWD changes. `-p` (also `--parents`) creates missing ancestors
and accepts existing directories. Without it, missing parents and existing
destinations are errors. SFTP accepts symlinks to directories with `-p` but rejects
file collisions. Permission failures remain errors; successful commands are
silent. TAB completes existing directories, including after `-p` and earlier
operands.

Creation runs as a cancellable job and refreshes the completion cache. `-p` can
safely retry after a transport failure using the normal backoff/reconnect policy.
Plain `mkdir` is not automatically replayed by the job: if the server creates the
directory but its reply is lost, inspect the result before retrying. Errors stop
at the first failed operand; directories already created are not rolled back.

S3 directories are virtual prefixes. New directories are zero-byte objects whose
keys end in `/`; `mkdir -p a/b/c` creates `a/`, `a/b/`, and `a/b/c/` when missing.
Prefixes already containing objects count as existing directories and are left
untouched with `-p`, without adding unnecessary markers. A bare object `a` and a
directory `a/` are distinct and may coexist; creating the latter never replaces
the former. Absolute S3 paths start with `/bucket/`. Buckets must already exist:
`mkdir` never creates buckets, and `mkdir -p /existing-bucket/` only validates it.

S3 requires `s3:ListBucket` and, for new markers, `s3:PutObject` permissions. Marker
writes use `If-None-Match: *` to prevent overwriting any existing trailing-slash
object, including concurrent creations. The endpoint must support conditional
PutObject; xfer does not fall back to unconditional writes. Existence checks for
implicit prefixes are not atomic with concurrent writers, but existing objects
are never replaced. Invalid requests, access denial, and missing buckets fail
without entering an endless retry loop.

## Remote directory usage

`du [paths...]` recursively lists the byte total for each remote directory,
children before parents. With no path it uses the current remote directory;
`du -s [paths...]` prints only each selected root's total. Add `-h` (or `-sh`)
for human-readable sizes using binary units such as KiB, MiB, and GiB. Individual files are
also accepted. For example:

```text
du
du -s downloads archive
du -- -directory
```

Totals are apparent file bytes, not allocated disk blocks: directory attributes
are excluded, descendant symlinks are counted without following them, and hard
links are counted per directory entry. Missing file sizes or listing failures
report an error rather than a partial total. Traversal runs as a cancellable job
with normal transport retries; it does not change the remote working directory.

## Command history

`ui:history-file` defaults to `history` in the application state directory
listed above (`$XDG_STATE_HOME/xfer/history` or `~/.local/state/xfer/history`
on Linux). Set it to `session` to retain history only while the process runs,
or `/dev/null` on Unix to disable history entirely. Set it in the application
config directory's `xfer.conf` before startup, for example:

```text
ui:history-file: session
```


## Daemon process

The same `xfer` executable runs the per-session daemon with `xfer daemon`.
The binary is a clap-based CLI: `xfer` starts a daemon and attaches, `xfer
attach [id]` attaches to an existing daemon by instance-ID prefix, `xfer daemon`
runs only the daemon loop (spawned by the client), and `--help`/`--version` are
supported. The SSH askpass bridge is selected by the environment, not arguments.

The daemon refuses direct shell execution: it verifies its parent process is the
same xfer executable (a usability guard, not a security boundary; test harnesses
override it with the documented environment variable).

Normal `xfer` startup spawns that mode automatically. After initialization, the
daemon publishes its discovery metadata atomically and sends a single JSON
readiness message on its private stdout pipe. The parent waits up to five
seconds for readiness, then connects directly to the announced socket; it never
uses discovery or ID-prefix matching for this initial attachment. On Linux,
macOS, FreeBSD, NetBSD, and OpenBSD it also verifies the kernel-reported socket
peer PID against the spawned child: `SO_PEERCRED` on Linux/OpenBSD,
`LOCAL_PEERPID` on macOS, `LOCAL_PEERCRED` on FreeBSD (including the `xucred`
version check), and `LOCAL_PEEREID` on NetBSD.
Unavailable or invalid credentials fail startup on those platforms; matching only
the peer UID is not sufficient. Failed startups are killed and reaped rather
than left running after a timeout. The separate parent-executable guard still
uses Linux `/proc`, so these peer checks alone are not a complete BSD/macOS port.

The connected socket uses versioned, length-prefixed **Protobuf RPC**, defined in
[`proto/xfer.proto`](proto/xfer.proto). Typed requests/results cover command
control, sessions, completion, settings, jobs/progress, errors, and binary output;
the TUI no longer parses wire markers such as `OUTPUT` or `DONE`. The shared Rust
client lives in `src/ipc.rs`, with no GUI dependencies. Cargo generates bindings
using a Rust compiler (`protox`), so no system `protoc` is needed to build xfer.
See [the IPC contract](docs/ipc.md) for framing, limits, Java binding generation,
credential handling, and future GUI extension points. Restart older daemons:
the former text RPC protocol is not kept as a parallel compatibility mode.

Each daemon binds a socket in its own uniquely allocated private instance
directory; it never unlinks another instance's socket to make room. Discovery is
read-only and remains available for `xfer attach`; ambiguous ID prefixes are
rejected. Legacy discovery metadata is still readable. Normal shutdown removes
metadata before the socket directory; crash remnants may remain for separately
coordinated cleanup. There is no separate `xferd` binary.

## Local shell commands

Start a line with `!` to run the rest verbatim using the installed `bash -c`, locally
in **Local CWD** (the directory selected with `lcd`):

```text
!pwd
!echo ok
!vim test.c
!cat file.txt
!ls -la
!ls -la | grep something
```

Bash handles quoting, variables, redirection and pipelines. Input, stdout and stderr
are attached directly to the terminal, so editors work and output is not paged or
captured as a daemon job. Ctrl-C interrupts the local command without exiting xfer.
A failure reports the exit status and returns to the prompt. Each invocation is a
separate shell: `!cd ...` and `!export ...` do not change xfer's Local CWD or environment.
The client fetches Local CWD from the daemon for every invocation, so `lcd` takes
effect immediately for `!pwd`, relative file paths, redirects, and local editors.
Only the child Bash process changes directory; configuration paths and xfer's own
process CWD remain unchanged. If Local CWD has been deleted or cannot be entered,
the command reports an error rather than falling back to the startup directory.
The original `!` line follows normal command history rules; rc lines use the same
execution path without being added to history. A bare `!` prints usage.

## SFTP connections

SFTP uses the installed `ssh` executable and the binary SFTP subsystem (not shell
commands or parsed `ls` output). It respects OpenSSH configuration, host aliases,
SSH agents, identity files and `ProxyJump`. URLs are
`sftp://[user@]host[:port][/absolute/directory]`; omitting the path starts in the
server's login directory. URL paths `/~` and `/~/...` select the remote home, e.g.
`sftp://seedhost/~` or `sftp://seedhost/~/downloads`. OpenSSH's `expand-path`
extension expands `~` on the server; without it, these paths resolve relative to
the server's initial directory (usually the login home). SFTP does not expose
`$HOME` or other environment variables, and the client's local home is never used.
`cd` without an argument returns to the initial directory selected by the URL.
Listings, file/directory completion, symlinked directories and `get [-c]` work
through the same daemon jobs, cache and transfer UI as S3. Remote names must be UTF-8.
SFTP `ls -l` output includes symbolic permissions (file type and special bits),
numeric server UID/GID, UTC modification time, and exact byte size, for example:

```text
-rw-r-----       1000       1001 [2026-10-16 13:22:00 UTC]       4096  notes.txt
```

SFTP v3 provides numeric owner/group IDs. When a server supplies the optional,
implementation-defined `longname` listing text in the conventional `ls -l` form,
xfer displays its owner and group names; it otherwise displays numeric IDs. xfer
never resolves remote IDs using the local account database. Missing attributes are
shown as `-` (or `??????????` for an unknown mode). Directory sizes are the
server's directory attribute, not a recursive total. Symlink entries retain their
own metadata, even when their target is a directory offered by completion.

For agent/key authentication, use `open sftp://user@host` or a bookmark without
`password_interactive`. Encrypted private keys should be loaded into `ssh-agent`.
For an interactive login password, add this bookmark to `~/.config/xfer/bookmarks.toml`:

```toml
[server]
type = 'sftp'
url = 'sftp://user@my-ssh-alias'
password_interactive = true
```

`open server` (or `connect server`) asks for the password **before starting the
connection job**, without echoing it. Escape/Ctrl-C cancels; Backspace edits and
Ctrl-U clears it. The password is sent over the private daemon socket, then supplied
only when OpenSSH requests it using a short-lived private askpass socket. It is
not stored in bookmarks/history/job output, process arguments or environment
variables. The daemon retains the password **in memory for the lifetime of the
logical SFTP connection**, including across client detach/reattach, so transport
recovery can authenticate without another prompt. Closing or replacing the
connection releases it; the secret buffer is zeroized when its final reference is
dropped. Askpass sockets exist only during authentication and are removed afterward.
A wrong password fails the job, including during recovery; run `open server` again
to enter a new one. Startup rc commands use the same prompting path and require an
attached terminal.

Host-key verification is always strict, independent of `ssl:skip-validation`.
Connect using normal `ssh` first to verify/trust a new host key. xfer never supplies
a password as a host-key confirmation or private-key passphrase. Keyboard-interactive
MFA/challenge-response authentication is not supported by this single-password flow.
`sftp:connect-timeout` bounds SSH connection/authentication (default 30 seconds).
Authentication, host-key and remote filesystem errors fail immediately; transient
SSH connection failures use the normal retry backoff. An established transport
failure or operation timeout discards the dead session. On the next retry, xfer
starts a fresh SSH/SFTP session using the retained connection settings and password
(or keys/agent), without resetting the remote CWD or initial home directory.
`get`, `ls`, and `cd` use the existing 3, 6, 12, 24, then 30-second backoff; there is
no separate nested retry loop inside SFTP. Interrupted downloads reopen the file
and resume from the last locally written byte after checking its original size and
mtime. Cancellation also drops an in-flight session, leaving the next operation
able to reconnect. Filesystem, changed-file, authentication, and host-key errors
remain non-retryable. Connection errors include the target, failed setup step,
underlying cause, SSH exit status (when available), and captured SSH stderr;
supplied passwords are redacted.

Downloads and uploads pipeline up to **64 concurrent requests** by default,
using bounded 64 KiB application chunks rather than waiting for each individual
chunk's round trip. The protocol library respects the server's negotiated
read/write limits, including older servers with smaller packets. Downloads write
completed chunks in file order, so interrupted transfers retain a contiguous
prefix for exact resume. Uploads use explicit non-overlapping offsets, zero-copy
protocol buffers, and wait for every write acknowledgement and close before
reporting success. New requests are submitted immediately as the window refills.

`sftp:max-requests` accepts **1..256** (default **64**). It is frozen when connecting;
reconnect after changing it. For a high-latency/high-bandwidth link, try `128` or
`256`; `1` restores serial operation for troubleshooting. The default window holds
at most 4 MiB of chunk payload (16 MiB at 256), plus bounded protocol buffers.
Actual throughput still depends on the SSH channel, encryption CPU, server, disk,
and network; no setting can exceed their limits. Rebuild and restart an old
running daemon to use the new transfer code—reattaching alone does not update it.

Uploads open a regular local file without following its final symlink, freeze its
length, and check size plus high-resolution modification/change times afterward.
A changed source fails instead of silently growing/truncating the upload. These
checks are not an atomic snapshot. Failed/cancelled uploads can leave remote
partial files and do not resume in place; exclusive creation remains the default,
with overwrite only when `xfer:clobber` is enabled. Interrupted pipelines discard
their session even for permanent local errors, preventing abandoned requests from
contaminating the next operation while preserving the original error/retry policy.

The deterministic SFTP protocol test models 65 ms reply latency, compares request
windows 1/16/64 in both directions, and checks exact bytes, short/out-of-order
responses, clobber behavior, changed files, cancellation, and legacy packet limits.
Run it with `cargo test --lib sftp::transfer::tests -- --nocapture`. This is a
controlled latency test, not a claimed speed for every remote server.

SFTP downloads compare the opened file's size and modification time before/after
reading and on retries. Unlike S3's `If-Match` conditional reads, these checks are
not atomic and timestamps have second resolution: same-size changes with an
unchanged timestamp may go undetected. A pre-existing local partial file is still
the user's responsibility when using `get -c`.

## S3 connections (first backend)

`connect s3://bucket` selects the backend by its registered `s3` protocol token.
The AWS Rust SDK uses its default credential chain (environment, shared AWS
config, workload/instance credentials). `disconnect` releases the active logical
connection. S3 downloads use `get` (see below). Connection changes run as
cancellable daemon jobs; failed connects preserve the previous connection.

`connect` probes bucket access using `HeadBucket`. For an endpoint-only bookmark,
`ListBuckets` validates account access instead. This requires the corresponding
permissions; specify `bucket` to use a bucket-level probe. S3 has no persistent
login/session to close: disconnect drops the SDK client.

### Bookmarks

Use TOML, not a custom DSL. The default is `bookmarks.toml` in the application
config directory listed above (`$XDG_CONFIG_HOME/xfer/bookmarks.toml` or
`~/.config/xfer/bookmarks.toml` on Linux). Override with `bookmarks:file` in
`xfer.conf`/`XFER_CONFIG` or `set`.

`type` is optional when the URL scheme identifies the backend: `sftp`, `s3`,
`smb`, `dav`, `davs`, `webdav`, or `webdavs`. Plain `http`/`https` is shared by
S3-compatible endpoints and WebDAV, so a `bucket` or `accessKey` field selects
S3 and any other `http`/`https` bookmark selects WebDAV. An explicit `type`
always wins and must match the URL scheme. Bookmarks with other schemes (or no
scheme) require an explicit `type`, which must be a supported backend.

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
Long output opens the pager when `ui:auto-pager` is enabled:

- `j`/Down and `k`/Up scroll one line.
- Ctrl-F, PageDown, or Space scroll down a page; Ctrl-B or PageUp scroll up a page.
- Home/End jump to the first/last screen; `q` or Escape closes the pager.
- `|` opens the **full output** in `$VISUAL`, falling back to `$EDITOR`, then `vi`.
  The editor gets a private temporary file as its final argument and keeps terminal
  stdin for interactive use. Quoted editor arguments are supported; shell pipelines
  are not evaluated. On exit, the pager returns to the same position. Editor changes
  do not alter the original output, and the temporary copy is removed afterward.

Once displayed or dismissed, output and the finished job are removed. Ctrl-Z during
listing leaves the job in the background for later `fg`.

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

Retryable backend errors from `connect` and `ls` are retried indefinitely; SFTP
non-retryable errors are described above. Delays are
3, 6, 12, 24, then 30 seconds between attempts, staying at 30 seconds until the
operation succeeds or is cancelled. Foreground progress includes the last error
and the next delay. Ctrl-C cancels either an active request or a backoff wait;
background jobs can be cancelled with `cancel <id>`. SDK-internal retries are
disabled, avoiding nested backoff policies.

Each failed listing attempt's partial output is discarded before retrying, so
restarting a paginated listing does not duplicate entries. Connection attempts
keep their settings snapshot and preserve an existing connection on failure.
Syntax errors/unknown bookmarks are still reported immediately before a job
starts. Downloads retry from the last locally written byte and retain their
initial remote revision; future upload/write operations will need their own
idempotency/resume handling rather than blindly replaying writes.

### `ls` output and sorting

```text
ls [-FlarSh] [--] [path]
```

The string setting `ui:ls-default-options` supplies base flags, defaulting to `-lF`
(long output with directory suffixes). Command-line flags are applied afterward,
overriding individual defaults; options not mentioned on the command line remain
in effect. Changes apply to new listings without reconnecting and do not alter
already-running jobs or retries.

```text
set ui:ls-default-options '-lF'
ls -S                         # long/classified output, sorted by size
ls --no-long --no-classify     # bare names for this command only
set ui:ls-default-options ''     # bare names by default
```

In `xfer.conf`, use `ui:ls-default-options: -lF` (or an empty value to disable base
flags). Quote interactive values containing separate flags, e.g.
`set ui:ls-default-options '-lF -a'`. Defaults must contain options only, not a path.

Without `-l`/`-F`, `ls` prints one bare name per line. Listings have no summary
footer. Dotfiles and dot-directories are hidden unless `-a` is enabled.
Unless changed by sorting flags, directories (including directory symlinks) and
buckets come first, then files, with names sorted lexically A–Z within each group.

- `-F`: append `/` to directory and bucket names (no other type suffixes).
- `-l`: long output with dates, exact byte sizes, and available Unix metadata.
  Directory suffixes still require `-F`.
- `-h`: show sizes in binary human-readable units (KiB, MiB, etc.) in long output.
  Size sorting still uses exact byte counts.
- `-a`: include hidden dotfiles/directories. `.` and `..` are not synthesized.
  Explicitly named hidden files are shown even without `-a`.
- `-S`: sort all items by size, largest first, then by name for ties. Unknown
  sizes (e.g. virtual S3 directories/buckets) come last, ordered by name.
- `-r`: reverse the entire selected order, including directory/file groups or
  unknown-size placement.

Use `--no-long`, `--no-classify`, `--no-all`, `--no-size`, `--no-reverse`,
or `--no-human-readable` to
clear the corresponding inherited flag. Later flags win, so `--no-long -l` enables
long output again. `--no-size` restores name sorting.

Flags can be combined (`ls -laFSr`) or separate (`ls -l -a`). Use `--` before a
path beginning with `-`, e.g. `ls -l -- -filename`. Display filtering does not
remove hidden entries or metadata from the shared completion cache. Each retry
restarts the listing with the same options and resolved remote path.

### Listing records and display

Backends emit typed `Content` records (`Bucket`, `Directory`, `File`, `Comment`)
through a `ListingSink`, one entry at a time. `src/listing.rs` defines these types
and the common renderer; backends do not format terminal output. `ls` collects
small sorted runs (at most 2048 records/about 1 MiB) and merges them using a private
temporary directory, then renders the chosen view to the output spool. Small
listings stay in memory; large/paginated listings do not require retaining all
records or opening all runs at once. Temporary runs are removed after success,
error, or cancellation. Completion/picker sinks can consume the same metadata
without parsing text.

Example `ls -lF` output (fixed-width date and size columns):

```text
[2025-04-16 10:11:00 UTC]          -  ci-production/
[                      -]          -  src/
[2026-10-16 13:22:00 UTC]       4096  notes.txt
```

S3 object dates are `LastModified` values from `ListObjectsV2`; bucket dates are
creation dates from `ListBuckets`, not modification times. Virtual directory
prefixes have no timestamp, and neither buckets nor prefixes have an aggregate
size in the listing API: these fields display `-` rather than fabricated dates
or zero sizes. All dates are UTC; listing sizes are exact byte counts, not rounded
human-readable units. Permissions and ownership are omitted for S3, since ordinary
S3 listing metadata does not contain them.
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
ls -F               # names relative to photos/: sub/, a.jpg, ...
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
unlimited and sorts/spools its results independently, even if the cached snapshot
is partial.
Failed refreshes invalidate their old snapshot rather than caching partial errors.

Prompt editing supports Left/Right to move the caret, insertion at the caret, and
Backspace to erase the preceding character. Home/Ctrl-A go to the beginning;
End/Ctrl-E go to the end. Ctrl-L clears the screen and redraws the prompt and
command without changing the text or caret. Delete/Ctrl-D erase the character
under the caret; Ctrl-D on an empty line detaches. Ctrl-W erases whitespace and
the preceding word before the caret, preserving later text. Up/Down recall history
with the caret at the end. Long lines scroll horizontally; wide Unicode characters
are accounted for when positioning the caret, and long prompts are shortened to
leave editing room.

TAB completes the token at the caret, preserving later arguments (including when
completing inside a quoted path). It extends to the longest shared prefix of all
matches, for commands, setting names, and remote paths. For example, `cd bui<TAB>`
with `builddylib-ci/` and `builddylib-remote-ci/` becomes `cd builddylib-`.
Common prefixes are computed before shell quoting, on Unicode character
boundaries. A partial/limited cached listing does not auto-extend the prefix,
since it may be missing other matches.

### Ctrl-S filename selector

At the command prompt, **Ctrl-S** opens a full-screen text listbox for the
current directory. It is non-recursive and includes hidden entries:

```text
> [X] first file.txt
  [ ] folder/
  [X] 日本語.txt
```

- **Up/Down** or **j/k** moves the focus; **PageUp/Ctrl-B**, **PageDown/Ctrl-F**,
  and **Home/End** scroll.
- **Space** selects/deselects the focused entry.
- **s** cycles sorting: **name A–Z** (default), **name Z–A**, **largest first**,
  **smallest first**. Names use case-sensitive Unicode ordering; equal sizes
  are ordered by name, and unavailable sizes always sort last.
- **/** enters live regexp filtering, initially `.*` (everything). The first
  typed text replaces the previous pattern; Backspace edits and Ctrl-U clears.
  Results update while typing. **Enter** applies the filter without closing the
  dialog; **Esc** reverts to the previously applied filter. Invalid expressions
  show an error, keep the last valid preview, and cannot be applied.
- Outside filtering mode, **Enter** appends selected names as separate,
  shell-quoted arguments. It does **not** execute the command; press Enter again
  at the command prompt to submit it.
- Outside filtering mode, **Esc** or **Ctrl-C** cancels, preserving the original
  command and caret.
  Accepting an empty selection also leaves the command unchanged.

`put` selects regular files in the daemon's logical Local CWD, excluding final
symlinks and special files. `lcd` selects local directories; `cd` and other
remote-directory commands select remote directories. `!` commands use the
logical Local CWD. Other commands use the remote directory when connected,
otherwise the local directory. Directories are displayed/inserted with `/`.

The size column shows human-readable file sizes, `<DIR>` for directories, and
`?` when a file size is unavailable. The footer counts checked files and sums
their known sizes, including checked entries hidden by the current regexp.
Directories are counted separately and never scanned recursively for totals;
unknown file sizes are reported explicitly rather than treated as zero. Empty
files have a known size of `0 B`. Totals use overflow-safe arithmetic.

Acceptance always preserves the existing command and all manually entered
filenames, appending at the end regardless of caret position. A separator is
added when needed, so `get manual.txt` cannot become `get manual.txtselected.txt`.
Unfinished quotes/escapes must be closed before appending; otherwise the line is
left unchanged with a diagnostic. Cancelling preserves both text and caret.
Names starting with `-` or `~`, or containing `*`/`?`, get an explicit `./` prefix so they cannot
be mistaken for options, home expansion, or basename wildcard patterns.
`get`/`put` treat `./`-prefixed operands literally, including wildcard characters.

Regexp matching uses Rust `regex` syntax on basenames (case-sensitive by
default; `(?i)` enables case-insensitive matching). Patterns are limited to
4096 UTF-8 bytes with bounded regex compilation memory; look-around and
backreferences are unsupported. Filtering streams over the original disk-backed
snapshot in cancellable jobs; sorting uses bounded-memory external merge runs.
Neither operation rescans the remote server or truncates to
TAB's completion limit. Selections retain their identity across filters, even
when filtered out, and all selected entries are appended on acceptance.

Each opening takes a fresh disk-backed snapshot, independent of TAB's bounded
completion cache. Entries initially sort by name A–Z; there is no fixed entry-count
limit, and only a visible page is held in client memory. Loading is cancellable;
partial/failed listings are never offered as complete results. Unsafe control
characters, non-UTF-8 names and unrepresentable names are skipped with a count.
Selections are limited to 64 KiB of quoted arguments, and insertion must fit the
1 MiB IPC command limit. Closing the selector releases its private snapshot.
Terminal resizing is supported, and the original screen and input mode are
restored on close. Ctrl-S is not active in password prompts or foreground jobs.

The real-terminal regression test is `cargo test --test selector_pty`; it needs
Python 3 and an installed OpenSSH `sftp-server` and otherwise skips explicitly.

### Prompt and daemon status

The prompt shows the committed backend working directory, not a process ID:

```text
xfer> open minio
xfer[s3:/]> cd archive
xfer[s3:/archive/]> cd photos
xfer[s3:/archive/photos/]> status
```

Configure the prompt with the string setting `ui:prompt`, either interactively:

```text
set ui:prompt 'xfer[$cwd]> '
set ui:prompt '$user@$host [$cwdn]> '
```

or in `xfer.conf`: `ui:prompt: xfer[$cwd]> `.

| Variable | Expansion |
| --- | --- |
| `$cwd` | Backend and full remote directory, e.g. `sftp:/home/antek/` |
| `$cwdn` | Last component of the remote directory (`/` at root) |
| `$user` | Configured remote username, if known |
| `$host` | Remote hostname / configured S3 endpoint hostname, if known |
| `$proto` | Backend protocol, e.g. `sftp` or `s3` |
| `$lcwd` | Full local working directory |
| `$lcwdn` | Last component of the local working directory |
| `$location` | `[$cwd]` when connected, empty when disconnected |

The default is `xfer$location> `, preserving the prompts above. Remote variables
are empty when unavailable; `$user` does not expose S3 credentials or infer a
username from SSH configuration. `$host` is empty for S3's automatic AWS endpoint.
`${name}` also works, and `$$` prints a literal `$`. Unknown variables stay literal.
Expansion is single-pass (not shell evaluation); terminal control characters are
escaped. Include any desired trailing space in the template.

`status` displays the connected daemon's PID, backend, CWD, and running-job count,
plus absolute paths for the user config, the `XFER_CONFIG` override (if set), and
the current `bookmarks:file`. Config files missing at startup are explicitly marked
`not loaded: file not found`. It also shows the rc file if it supplied startup
commands; that path is remembered across client reattachments.
The prompt refreshes before each new command line after connect/cd/close or
reattach. It reads a lightweight session snapshot without creating a job or
waiting for a background operation to release the backend connection lock.

### `ls` path completion

`ls <TAB>` uses the same typed cache as `cd`, but includes files as well as
buckets, directories, and `../`. Directories/buckets get a trailing `/`; files
do not. Common-prefix completion, nested/absolute paths, quoting, and cache
expiry/refetch work for both commands. Completion preserves `ls` flags and `--`,
e.g. `ls -laF photos/im<TAB>`. `cd` continues to exclude files.

```text
ls READ<TAB>           # README.txt
ls photos/im<TAB>      # photos/image.jpg
ls -l README.txt       # show metadata for this exact file
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


## Remote file contents

`cat [--] <files...>` prints remote file contents to stdout using the active
S3/SFTP/SMB connection:

```text
cat README.txt
cat 'first file.txt' /absolute/path/second.txt
cat -- -filename
```

Relative paths use remote `pwd`, not Local CWD. Files are concatenated in argument
order, without headings, separators, success footers, UTF-8 conversion, or the
pager. No options other than `--` are accepted; this does not read local files or
stdin (`!cat ...` runs local Bash instead). TAB completes each remote filename.

Contents are first spooled to a private temporary file (0700 directory, 0600 file),
then copied to stdout in bounded chunks when the job finishes. Network failures
use the existing backoff/reconnect policy and resume from the last written byte,
with the backend's size/revision checks. Errors stop the command; bytes from prior
files or a partial read may still be printed, but diagnostics go to stderr and
never become part of the file bytes. Ctrl-C cancels fetching; Ctrl-Z backgrounds
it, and `fg` retains raw-output behavior. Temporary contents are removed when the
job output is acknowledged, or on daemon exit.

Contents are emitted verbatim, including terminal control sequences: only display
trusted files. Ordinary terminal newline processing still applies. If the final
byte is not a newline, the interactive UI puts its next prompt on a separate line.

## Uploads and local file completion

`put [--] <files...>` uploads regular local files to the current remote SFTP/SMB
directory. Local relative paths use Local CWD (selected with `lcd`); absolute
paths and literal `~/...` paths are also accepted.

TAB completes **regular local files only**, for each operand, including after
`--`. It reads Local CWD afresh, independent of the remote connection, and works
before connecting. Directory names, final symlinks, and special files are not
suggested, matching upload preflight. For nested paths, type the parent directory
prefix before completing the filename. Spaces and Unicode are quoted safely,
hidden files require a `.` prefix, and completion at the caret preserves later
arguments.

```text
put report.txt
put 'first file.txt' second.txt
put ~/Downloads/report.txt
put -- -filename
```

Uploads preserve each basename on the server. Successful uploads invalidate the
remote completion cache so new files are visible immediately. S3 uploads remain
unsupported; S3 directory markers from `mkdir` are separate from file uploads.

## Downloads

```text
get README.txt photos/image.jpg
get 'file with spaces.txt'
get -c large.iso
set xfer:clobber true
get README.txt
```

`get [-c] [--] <filenames...>` downloads sequentially to the **daemon's local
working directory**, preserving each basename (`photos/image.jpg` becomes
`./image.jpg`). Relative sources resolve against backend `pwd`; absolute sources
such as `/bucket/path/file` also work. The client initializes the daemon's local
directory from its own CWD on each attach. `status` shows Local CWD.

`lcd [directory]` changes this local directory for subsequent downloads, uploads,
and `!` shell commands. Relative
paths resolve against the previous Local CWD; absolute paths, `..`, quoted names
with spaces, and `~`/`~/...` are supported. With no argument, `lcd` selects the
daemon's `$HOME`. The directory must already exist; errors leave Local CWD
unchanged. Symlinks are resolved to their physical destination, so a `Downloads`
symlink may display its target directory instead. This does not change remote CWD
or the client/daemon process-wide CWD used for configuration paths.

TAB completes local directories against the current Local CWD, not remote `pwd`
or the client's startup directory. Relative, absolute, `../`, and `~/` paths,
quoted spaces, Unicode, and symlinks to directories work. Files and broken links
are not suggested; hidden directories are suggested when the filename prefix
starts with `.`. Completion reads the filesystem afresh and works disconnected.
Matching results are bounded; incomplete sets are marked and do not auto-extend
the command to a potentially misleading common prefix.

```text
lcd ~/Downloads
lcd 'photo album'
lcd ..
lcd                 # local home directory
```

Every download job snapshots its destination, so `lcd` or attaching another client
elsewhere does not redirect an existing background job. A later attach initializes
Local CWD again from that client's directory.

`xfer:clobber` defaults to `false`. Existing local files cause an error suggesting
`set xfer:clobber true`; enabling it allows replacement. `get -c` instead resumes
from the existing local size regardless of clobber, creates missing files from
scratch, skips already complete files, and rejects files larger than the remote
object. Duplicate destination basenames in one command, symlinks/non-regular
files, and destinations locked by another download are rejected. Local conflicts
are checked before remote requests or truncation. Partial files keep their final
names and remain after errors/cancellation, ready for `get -c`.

TAB completes each filename argument, including after `-c`, using the same typed
cache as `ls`. Directories/buckets remain available for completing nested paths;
quoted names and common-prefix expansion work with multiple arguments.

During transfer, the foreground status shows a progress bar, aggregate bytes and
percentage, elapsed time (`t`), a recent throughput estimate (about 2 seconds),
and ETA. Resumed bytes count toward completion but not new network throughput.
Ctrl-C cancels; Ctrl-Z backgrounds the job; normal typeahead still works.

Downloads stream in bounded chunks, without loading objects into memory. Network
failures use the 3/6/12/24/30-second indefinite retry policy and automatically
continue from bytes already written. Files completed earlier in the same command
are not downloaded again. S3 uses `HeadObject` for size/ETag, and `GetObject` with
`If-Match` and validated byte ranges to avoid appending a full/changed response.
A changed remote revision, invalid range, missing file, or local safety error
stops the job. Stalled response bodies time out after 30 seconds.

For an existing file supplied to `get -c`, the user must ensure it is a prefix of
the intended remote object: length alone cannot prove that existing local bytes
match. The ETag guard protects requests/retries within this job, not the provenance
of a pre-existing local partial file. Enabling clobber intentionally truncates the
old file; partial downloads are not atomically renamed into place.

## Command implementation

`src/commands.rs` defines the `Command` trait and the global immutable `COMMANDS`
table. Both the attached client's dispatcher and the daemon walk this table to
match parsed input. Help, aliases, command-name completion, argument-completion
policy, and history policy come from the same entries rather than separate lists.
Help and command-name completion sort names alphabetically, including aliases.

Small commands (`help`, `lcd`, `pwd`, `show`, `set`, jobs, and client session controls) are
grouped in `commands.rs` using `SmallCommand`, which implements `Command`. Larger
handlers live in dedicated modules under `src/commands/`: `connect.rs`, `cat.rs`, `get.rs`,
`ls.rs`, `mkdir.rs`, `cd.rs`, `bookmark.rs`, `status.rs`, `output.rs`, and `shell.rs`.

Each implementation provides `names()` (canonical name and aliases) and
`handler()`, returning a function with this signature:

```rust,ignore
fn execute(
    context: &mut CommandContext<'_>,
    parser: &ParserContext<'_>,
) -> anyhow::Result<CommandResult>
```

`CommandContext` contains either the authoritative daemon app or a `ClientApp`
adapter for terminal/session services. Execution-site checks prevent client-local
commands from running in the daemon. Passwords are supplied out of band in the
context, never in parser text. `ParserContext` provides the original source,
command name, decoded arguments, and unmodified Bash source for `!` escapes.

To add a large command, create its module, implement `Command`, and add a single
entry to `COMMANDS`. Optional `help()`, `completion()`, `site()`, and
`record_history()` methods customize its behavior. Internal spool commands remain
registered but are hidden from help and command-name completion. Interactive and
startup commands use the same registry and handlers.
