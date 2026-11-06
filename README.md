# xfer

[Homepage](https://anadoxin.org/xfer)

`xfer` is an interactive, terminal-based remote file-transfer client written in
Rust. It keeps a persistent connection in a small per-session daemon while the
terminal client provides an lftp-style command prompt, remote navigation,
listings, transfers, job control, completion, and a pager.

Its user experience is loosely modeled after [lftp](https://lftp.yar.ru/):
connect to a remote site, work with a remote current directory, and run commands
such as `ls`, `cd`, `get`, `put`, and `mkdir`. The implementation is different:
xfer uses async Rust, while lftp has a very nice cooperative task framework.
xfer is not a drop-in replacement for lftp.

Please have in mind that it was vibe-coded; it's working, but I would appreciate
pull requests that increase the code quality, which currently is... not the best.
However, I don't have that much time to have this as a full time project, I just
needed a tool that supports sftp, smb and s3.

xfer supports Linux, macOS, and FreeBSD. Windows is not supported.

## Demo

![xfer terminal demo](docs/xfer-demo.gif)

## Protocols

xfer supports these backends:

| Protocol | URLs | Read operations | Writes |
| --- | --- | --- | --- |
| S3 and S3-compatible storage | `s3://`, `http://`, `https://` | browse, `ls`, `get`, `cat`, `mirror` | `put`, `mirror-upload`, `mkdir`, `rm`, `mv` |
| SFTP | `sftp://` | browse, `ls`, `get`, `cat` | `put`, `mkdir`, `rm`, `mv` |
| SMB | `smb://` | browse, `ls`, `get`, `cat` | `put`, `mkdir`, `rm`, `mv` |
| WebDAV | `http://`, `https://`, `dav://`, `davs://`, `webdav://`, `webdavs://` | browse, `ls`, `get`, `cat` | `put`, `mkdir`, `rm`, `mv` |

S3-compatible HTTP(S) bookmarks must specify `type = 's3'` (or a `bucket` or
`accessKey` field), since ordinary HTTP(S) URLs otherwise select WebDAV.

## Getting started

Install the binary directly from GitHub (requires Rust 1.94.1 or newer):

```sh
cargo install --git https://github.com/antekone/xfer.git --locked
```

This installs `xfer` to `~/.cargo/bin` by default; it does not install the manual.
Prebuilt Linux and macOS archives, including the manual, and a PDF manual are
also available from [GitHub Releases](https://github.com/antekone/xfer/releases).

Alternatively, build the release binary and manual from a checkout (requires Cargo, groff, and gzip):

```sh
make
./target/release/xfer
```

Use `make release` or `make man` to build only the binary or manual.
The manual artifacts are `target/man/xfer.1.gz` and `target/man/xfer.txt`.
For a development build, run `cargo run`.

On FreeBSD, install the Rust toolchain, Bash, and Perl (for native dependency
builds), for example `pkg install rust bash perl5`. Install `gmake` and `groff`
if building the manual with the Makefile; use `gmake` instead of `make`. SFTP
uses the system OpenSSH client. FreeBSD CI runs the test suite on FreeBSD 15.1;
the SMB integration fixture currently runs only on Linux.

At the prompt, connect to a configured bookmark and work as you would in a
remote shell:

```text
xfer> open sftp-home
xfer[sftp:/home/alice/]> ls
xfer[sftp:/home/alice/]> get report.pdf
xfer[sftp:/home/alice/]> put local-notes.txt
```

Use `help` in the client for the command reference and `about` for version,
license, and build information. The build date is in UTC and honors
`SOURCE_DATE_EPOCH` for reproducible builds. `Ctrl-C` cancels a
foreground job; `Ctrl-Z` backgrounds it, and `jobs` / `fg` manage jobs.
Use `mirror .` to copy the current remote directory's contents into the current
local directory, or `mirror-upload .` to copy in the other direction. Named
directories are copied under their basenames. Neither command removes source
files or extra destination files.

`debug` shows the daemon's log level and private log file path. Use `debug 0`
(the default) for errors and warnings, `debug 1` for debug events, or `debug 2`
for trace events. By default the log is `xfer-debug-<pid>.log` in the
initial local directory. Set `xfer:debug-file` in the configuration or at the
prompt to change it, for example `set xfer:debug-file '${cwd}/shared.log'`.
The template supports `${pid}` and `${cwd}` (also `$pid` and `$cwd`); `cwd` is
the local directory when the setting is applied. Relative paths use that same
directory. Existing private files are appended to, so daemons can share a log;
each line includes the daemon PID. The file is created only when a message is
actually logged, and is not deleted automatically.
Diagnostics omit command arguments, job output, and error details because
these can contain sensitive data.

To execute a command file without an interactive terminal, run
`xfer -f commands.rc` (or `xfer --file commands.rc`). For example:

```text
# commands.rc
open archive
lcd ~/Downloads
get report.pdf
```

For a short inline script, use `xfer -s 'open archive; get report.pdf'`
(or `--script`); commands are separated by `;` outside quotes.

Commands run in order, waiting for each job. Script mode skips `~/.xferrc`,
does not record history, and cannot prompt for passwords. A failed job stops
the script with a nonzero exit status. Some commands report usage or setting
errors as text rather than failure status, so check their output as well. See
the manual's **Command scripts** section for details.

## Manual

The [xfer(1) manual page](docs/xfer.1) documents commands, settings, bookmarks,
and session management. View it from a checkout with:

```sh
man -l docs/xfer.1
# Or, after make man:
man -l target/man/xfer.1.gz
```

On systems without `man -l`, use `man ./docs/xfer.1` instead.

To install it for your user (on systems using `~/.local/share/man`):

```sh
mkdir -p ~/.local/share/man/man1
install -m 644 docs/xfer.1 ~/.local/share/man/man1/xfer.1
```

## Configuration

xfer reads `xfer.conf` from its application configuration directory, then reads
the file named by `XFER_CONFIG` if that environment variable is set. On Linux
and FreeBSD, the default directory is `~/.config/xfer`; the default bookmark file is
`~/.config/xfer/bookmarks.toml`.

`xfer.conf` uses `name: value` settings. This is a useful minimal example:

```text
# ~/.config/xfer/xfer.conf
ui:auto-pager: true
s3:region: us-east-1
sftp:connect-timeout: 30
sftp:disable-strict-checking: false
sftp:max-requests: 64
smb:connect-timeout: 10
smb:operation-timeout: 30
smb:max-credits: 32
ssl:skip-validation: false
```

Define connection targets in `bookmarks.toml`. The following example includes
one setup for every supported protocol; replace hosts, paths, and credentials
with your own values.

```toml
# ~/.config/xfer/bookmarks.toml

# S3: credentials may instead come from the standard AWS credential chain.
[archive]
type = 's3'
url = 's3://example-archive'
region = 'us-east-1'

# S3-compatible service (for example, MinIO).
[minio]
type = 's3'
url = 'https://minio.example.net'
bucket = 'backups'
accessKey = 'replace-me'
secretKey = 'replace-me'
region = 'us-east-1'
api = 's3v4'

# SFTP normally uses OpenSSH configuration, keys, and ssh-agent.
[sftp-home]
type = 'sftp'
url = 'sftp://alice@sftp.example.net/home/alice'

# Set password_interactive instead of storing a password in this file.
[fileserver]
type = 'smb'
url = 'smb://nas.example.net/Documents'
domain = 'WORKGROUP'
user = 'alice'
password_interactive = true

[web-files]
type = 'webdav'
url = 'https://dav.example.net/files/'
user = 'alice'
password_interactive = true
```

Connect with `open archive`, `open sftp-home`, `open fileserver`, or
`open web-files`. Bookmarks are read on each connection. Protect bookmark files
that contain credentials; interactive passwords avoid storing those passwords in
plain text.

## Limitations

- S3 has no real directories: `mkdir` creates zero-byte trailing-slash marker
  objects, and buckets must already exist.
- Moving an S3 object is **not atomic**. xfer implements `mv` as server-side copy
  followed by delete; a failure or cancellation can leave both source and
  destination objects. Objects up to 5 TiB are supported; objects larger than
  5 GiB use multipart copy. Prefix/directory moves are not implemented.
  Multipart copy requires permission to read object tags and abort multipart uploads.
- S3 uploads (`put`, `mirror-upload`, also `mirror-up`) stream from disk and
  support files up to 5 TiB. Files larger than 5 GiB use multipart uploads;
  replacing existing objects requires `xfer:clobber`. Failed or cancelled
  multipart uploads are aborted on a best-effort basis. Configure an S3
  lifecycle rule to clean up incomplete uploads and copies, which can incur storage charges
  if cleanup fails or the application stops unexpectedly. If completion fails,
  inspect the destination before retrying because its outcome may be unknown.
  `mirror` downloads S3 prefixes recursively.
- WebDAV requires a server with WebDAV `PROPFIND` support; an HTML directory
  listing alone is insufficient. Authentication supports Basic auth and
  anonymous access, not Digest, NTLM, OAuth/Bearer, or client certificates.
- SFTP password login is a single password flow; keyboard-interactive MFA and
  challenge-response authentication are not supported. SSH host-key checking is
  strict by default. Setting `sftp:disable-strict-checking: true` disables strict
  checking for new connections, allowing unknown host keys without verification;
  this is insecure and exposes connections to man-in-the-middle attacks. OpenSSH
  may still restrict authentication when a known host key changes.
- Remote operations may leave partial files after a failed or cancelled upload.
  Inspect ambiguous results before retrying mutating commands such as `mv` or
  `rm`.

For implementation and IPC details, see [docs/ipc.md](docs/ipc.md).

## Publishing a release

Push a tag such as `v0.3.0` to trigger the release workflow. It runs the test
suite on Linux, macOS, and FreeBSD before building archives containing the
binary and manual. If the tests and builds pass, it publishes the archives along with a PDF
manual, SHA-256 checksums and automatically generated release notes. Some
integration tests skip when their external test servers are unavailable.

For example:

```sh
git tag v0.3.0
git push origin v0.3.0
```

## License

xfer is licensed under the [GNU General Public License v3.0](LICENSE).
