# xfer

`xfer` is an interactive, terminal-based remote file-transfer client written in
Rust. It keeps a persistent connection in a small per-session daemon while the
terminal client provides an lftp-style command prompt, remote navigation,
listings, transfers, job control, completion, and a pager.

It is loosely modeled after [lftp](https://lftp.yar.ru/): connect to a remote
site, work with a remote current directory, and run commands such as `ls`,
`cd`, `get`, `put`, and `mkdir`. It is not a drop-in replacement for lftp.

Please have in mind that it was vibe-coded; it's working, but I would appreciate
pull requests that increase the code quality, which currently is... not the best.
However, I don't have that much time to have this as a full time project, I just
needed a tool that supports sftp, smb and s3.

xfer is intended for Linux and macOS. Windows support is planned and being
experimented with, but it probably does not work yet.

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

Build and run from a checkout:

```sh
cargo run
```

At the prompt, connect to a configured bookmark and work as you would in a
remote shell:

```text
xfer> open sftp-home
xfer[sftp:/home/alice/]> ls
xfer[sftp:/home/alice/]> get report.pdf
xfer[sftp:/home/alice/]> put local-notes.txt
```

Use `help` in the client for the command reference. `Ctrl-C` cancels a
foreground job; `Ctrl-Z` backgrounds it, and `jobs` / `fg` manage jobs.

## Configuration

xfer reads `xfer.conf` from its application configuration directory, then reads
the file named by `XFER_CONFIG` if that environment variable is set. On Linux,
the default directory is `~/.config/xfer`; the default bookmark file is
`~/.config/xfer/bookmarks.toml`.

`xfer.conf` uses `name: value` settings. This is a useful minimal example:

```text
# ~/.config/xfer/xfer.conf
ui:auto-pager: true
s3:region: us-east-1
sftp:connect-timeout: 30
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
  destination objects. It supports objects up to 5 GiB and does not implement
  prefix/directory moves or multipart copies of larger objects.
- S3 uploads (`put`, `mirror-upload`, also `mirror-up`) support files up to 5 GiB
  each; multipart uploads of larger files are not implemented. Uploads stream
  from disk and require `xfer:clobber` to replace existing objects. `mirror`
  downloads S3 prefixes recursively.
- WebDAV requires a server with WebDAV `PROPFIND` support; an HTML directory
  listing alone is insufficient. Authentication supports Basic auth and
  anonymous access, not Digest, NTLM, OAuth/Bearer, or client certificates.
- SFTP password login is a single password flow; keyboard-interactive MFA and
  challenge-response authentication are not supported. Host-key checking is
  always strict.
- Remote operations may leave partial files after a failed or cancelled upload.
  Inspect ambiguous results before retrying mutating commands such as `mv` or
  `rm`.

For implementation and IPC details, see [docs/ipc.md](docs/ipc.md).

## License

xfer is licensed under the [GNU General Public License v3.0](LICENSE).
