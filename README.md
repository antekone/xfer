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
