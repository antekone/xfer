# Standard-library-only PTY fixture; invoked by selector_pty.rs.
import fcntl
import json
import os
import pathlib
import pty
import re
import select
import shlex
import signal
import struct
import subprocess
import sys
import termios
import time

binary, server, directory = sys.argv[1:]
root = pathlib.Path(directory)
process = None
master = None
transcript = bytearray()
signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('PTY deadline')))
signal.alarm(45)
try:
    for name in ['home', 'config/xfer', 'cache', 'state', 'run', 'tmp', 'bin',
                 'remote/folder', 'local', 'downloads', 'filtered-downloads', 'large', 'empty']:
        (root / name).mkdir(parents=True, exist_ok=True)
    for name, data in [('hello world.txt', b'hello\x00\xff\n'), ('日本語.txt', b'unicode\n'),
                       ('literal*.txt', b'literal\n'), ("a'b", b'quote payload\n'),
                       ('.hidden', b'hidden\n'), ('folder/child.txt', b'nested')]:
        (root / 'remote' / name).write_bytes(data)
    (root / 'local/local.txt').write_text('local')
    (root / 'local/.hidden').write_text('hidden')
    (root / 'local/folder').mkdir()
    os.symlink('local.txt', root / 'local/file-link')
    for index in range(2505):
        (root / 'large' / ('file%04d.txt' % index)).write_bytes(b'')
    (root / 'bin/ssh').write_text('#!/bin/sh\nexec ' + shlex.quote(server) + ' -d "$TEST_REMOTE_ROOT"\n')
    (root / 'bin/ssh').chmod(0o700)
    (root / 'config/xfer/xfer.conf').write_text('ui:auto-pager: false\nui:history-file: session\nsftp:connect-timeout: 5\n')
    env = os.environ.copy()
    for name in ['XFER_CONFIG', 'XFER_SSH_ASKPASS_SOCKET', 'XFER_DAEMON_ALLOW_FOREIGN_PARENT']:
        env.pop(name, None)
    env.update(HOME=str(root / 'home'), XDG_CONFIG_HOME=str(root / 'config'),
               XDG_CACHE_HOME=str(root / 'cache'), XDG_STATE_HOME=str(root / 'state'),
               XDG_RUNTIME_DIR=str(root / 'run'), TMPDIR=str(root / 'tmp'),
               TEST_REMOTE_ROOT=str(root / 'remote'), PATH=str(root / 'bin') + ':' + env['PATH'],
               TERM='xterm-256color')
    master, slave = pty.openpty()
    original = termios.tcgetattr(slave)
    # Assertions below read the rendered command line, not the editor's hidden
    # horizontal-scroll buffer. macOS temporary paths can make the remote
    # prompt much longer than on Linux; keep the full command visible here.
    # The resize case below still exercises a narrow terminal explicitly.
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 512, 0, 0))
    process = subprocess.Popen([binary], cwd=root, env=env, stdin=slave, stdout=slave,
                               stderr=slave, start_new_session=True)
    os.close(slave)

    def send(value):
        os.write(master, value if isinstance(value, bytes) else value.encode())

    def until(pattern, mark=0, timeout=8):
        deadline = time.monotonic() + timeout
        while not re.search(pattern, bytes(transcript[mark:])):
            if time.monotonic() >= deadline:
                raise AssertionError(('waiting for', pattern, bytes(transcript[-5000:])))
            if select.select([master], [], [], .05)[0]:
                data = os.read(master, 65536)
                assert data, ('unexpected EOF', process.poll())
                transcript.extend(data)
        return bytes(transcript[mark:])

    frame = rb'\r\x1b\[2K(xfer[^\r\n]*?)\r\x1b\[[0-9]+C'
    prompt = rb'xfer(?:\[[^\r\n]*?\])?> (?=\r\x1b)'

    def line(mark):
        output = until(frame, mark)
        rendered = re.findall(frame, output)[-1].decode()
        return re.sub(r'^xfer(?:\[.*?\])?> ', '', rendered)

    def command(value):
        mark = len(transcript)
        send(value + '\r')
        return until(prompt, mark)

    def open_selector(value, source):
        mark = len(transcript)
        send(value + '\x13')
        until(rb'File selector', mark)
        until(rb'\d+ selected .*\d+/\d+', mark)
        output = bytes(transcript[mark:])
        assert ('— ' + source).encode() in output, output
        return mark

    def tick(value):
        mark = len(transcript)
        send(value)
        until(rb'\d+ selected .*\d+/\d+', mark)
        return mark

    def focus(mark):
        return re.findall(rb'> \[[ X]\] .{10}  ([^\x1b\r\n]*)', bytes(transcript[mark:]))[-1].decode()

    def choose(name, mark):
        for _ in range(20):
            if focus(mark) == name:
                return tick(' ')
            position, total = re.findall(rb'([0-9]+)/([0-9]+)', bytes(transcript))[-1]
            mark = tick(b'\x1b[H' if position == total else b'\x1b[B')
        raise AssertionError(('entry missing', name))

    def live_filter(pattern, total):
        mark = len(transcript)
        send('/')
        until(rb'\[editing: Enter applies', mark)
        mark = len(transcript)
        send(pattern)
        # Wait for a completed preview, not an intermediate/pending match count.
        until(rb'[0-9]+ selected \(incl hidden\).*?[0-9]+/' + str(total).encode()
              + rb' .*?[0-9]+ skipped\x1b', mark)
        return mark

    def applied_filter(pattern):
        mark = len(transcript)
        send(b'\r')
        until(re.escape(('Filter /' + pattern + '  [/ edits regexp]').encode()), mark)
        return mark

    def cycle_sort(label, expected_focus):
        mark = len(transcript)
        send('s')
        until(re.escape(('Sort: ' + label + ' [s cycles]').encode()), mark)
        # Identical leading rows need not be repainted when only sort changes.
        assert focus(0) == expected_focus, (label, focus(0))
        return mark

    def close_selector(key):
        mark = len(transcript)
        send(key)
        until(rb'\x1b\[\?1049l', mark)
        result = line(mark)
        restored = bytes(transcript[mark:]).split(b'\x1b[?1049l', 1)[1]
        assert b'\x1b[2J' not in restored, 'selector cleared the restored primary screen'
        return result

    until(prompt)
    command('lcd ' + shlex.quote(str(root / 'local')))
    mark = open_selector('', 'local')  # disconnected fallback, includes hidden entries
    assert b'.hidden' in transcript[mark:]
    mark = choose('file-link', mark)
    until(rb'1 files, 0 B known \+ 1 unknown', mark)
    mark = choose('folder/', mark)
    until(rb'1 files, 0 B known \+ 1 unknown \+ 1 dirs', mark)
    assert close_selector(b'\x1b') == ''
    command('open sftp://user@example.invalid')
    command('lcd ' + shlex.quote(str(root / 'downloads')))

    mark = open_selector('get ', 'remote')
    assert b'Sort: name A-Z' in transcript[mark:]
    assert focus(mark) == '.hidden'
    assert b'<DIR>' in transcript[mark:] and b'8 B' in transcript[mark:]
    cycle_sort('name Z-A','日本語.txt')
    cycle_sort('size largest',"a'b")
    cycle_sort('size smallest','.hidden')
    mark = cycle_sort('name A-Z','.hidden')
    assert b'.hidden' in transcript
    assert b'child.txt' not in transcript[mark:]
    mark = choose('日本語.txt', 0)
    mark = choose('literal*.txt', mark)
    until(rb'2 files, 16 B', mark)
    mark = tick(' ')
    until(rb'1 files, 8 B', mark)
    mark = tick(' ')
    until(rb'2 files, 16 B', mark)
    # Changing order must preserve selected checkboxes and totals.
    cycle_sort('name Z-A','日本語.txt')
    accepted = close_selector(b'\r')
    assert set(shlex.split(accepted)) == {'get', '日本語.txt', './literal*.txt'}, accepted
    assert not (root / 'downloads/日本語.txt').exists(), 'selector Enter executed command'
    mark = len(transcript)
    send(b'\r')
    until(prompt, mark)
    assert (root / 'downloads/日本語.txt').read_bytes() == b'unicode\n'
    assert (root / 'downloads/literal*.txt').read_bytes() == b'literal\n'

    # Live regexp previews, apply/revert, invalid expressions, stable selections,
    # and appending after a manual filename without a trailing space.
    command('lcd ' + shlex.quote(str(root / 'filtered-downloads')))
    mark = open_selector("get 'hello world.txt'", 'remote')
    assert b'Filter /.*' in transcript[mark:]
    mark = live_filter(r'^日本語\.txt$', 1)
    assert focus(mark) == '日本語.txt'
    applied_filter(r'^日本語\.txt$')
    tick(' ')
    mark = len(transcript)
    send('/[')
    until(rb'invalid regexp', mark)
    mark = len(transcript)
    send(b'\r\x1b')  # invalid Enter must not apply/exit; Esc reverts only the edit
    until(re.escape('Filter /^日本語\\.txt$  [/ edits regexp]'.encode()), mark)
    mark = live_filter(r'^missing$', 0)
    mark = len(transcript)
    send(b'\x1b')
    until(re.escape('Filter /^日本語\\.txt$  [/ edits regexp]'.encode()), mark)
    until(rb'1 selected .*1/1', mark)
    mark = live_filter(r'^literal', 1)
    assert focus(mark) == 'literal*.txt'
    applied_filter(r'^literal')
    tick(' ')
    accepted = close_selector(b'\r')
    accepted_words = shlex.split(accepted)
    assert accepted_words[:2] == ['get', 'hello world.txt'], accepted
    assert set(accepted_words[2:]) == {'日本語.txt', './literal*.txt'}, accepted
    assert not (root / 'filtered-downloads/hello world.txt').exists()
    mark = len(transcript)
    send(b'\r')
    until(prompt, mark)
    for name in ['hello world.txt', '日本語.txt', 'literal*.txt']:
        assert (root / 'filtered-downloads' / name).read_bytes() == (root / 'remote' / name).read_bytes()

    # Appending preserves every manual argument even with the caret inside one.
    value = "cat 'hello world.txt'"
    send(value)
    send(b'\x1b[H' + b'\x1b[C' * len("cat 'hel"))
    mark = open_selector('', 'remote')
    choose("a'b", mark)
    accepted = close_selector(b'\r')
    assert shlex.split(accepted) == ['cat', 'hello world.txt', "a'b"], accepted
    mark = len(transcript)
    send(b'\r')
    output = until(prompt, mark)
    assert b'quote payload' in output

    # Cancel discards toggles without moving the original caret.
    value = "cat 'hello world.txt'"
    send(value)
    send(b'\x1b[H' + b'\x1b[C' * len("cat 'hello"))
    mark = open_selector('', 'remote')
    tick(' ')
    assert close_selector(b'\x1b') == value
    mark = len(transcript)
    send('x')
    assert line(mark) == "cat 'hellox world.txt'"
    mark = len(transcript)
    send(b'\x03')
    until(prompt, mark)

    command('lcd ' + shlex.quote(str(root / 'local')))
    mark = open_selector('put ', 'local')
    assert b'.hidden' in transcript[mark:]
    assert b'file-link' not in transcript[mark:] and b'folder/' not in transcript[mark:]
    assert close_selector(b'\x1b') == 'put '
    mark = len(transcript)
    send(b'\x03')
    until(prompt, mark)

    command('lcd ' + shlex.quote(str(root / 'large')))
    mark = open_selector('put ', 'local')
    mark = tick('j')
    assert b'2/2505' in transcript[mark:]
    mark = tick('k')
    assert b'1/2505' in transcript[mark:]
    mark = tick(b'\x1b[B')
    assert b'2/2505' in transcript[mark:]
    mark = tick(b'\x1b[A')
    assert b'1/2505' in transcript[mark:]
    mark = tick(b'\x06')  # Ctrl-F is PageDown
    assert b'21/2505' in transcript[mark:]
    mark = tick(b'\x02')  # Ctrl-B is PageUp
    assert b'1/2505' in transcript[mark:]
    mark = tick(b'\x1b[F')
    assert b'2505/2505' in transcript[mark:], 'listing was capped at completion limit'
    tick(' ')
    tick(b'\x1b[5~')
    tick(b'\x1b[6~')
    mark = live_filter(r'^file2504\.txt$', 1)
    assert focus(mark) == 'file2504.txt'
    applied_filter(r'^file2504\.txt$')
    mark = len(transcript)
    fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', 9, 85, 0, 0))
    os.kill(process.pid, signal.SIGWINCH)
    until(rb'\x1b\[2J', mark)
    # Restore the wide viewport before comparing the full rendered command;
    # a long macOS prompt can scroll even "put " at the narrow size.
    mark = len(transcript)
    fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 512, 0, 0))
    os.kill(process.pid, signal.SIGWINCH)
    until(rb'\x1b\[2J', mark)
    assert close_selector(b'\x03') == 'put '
    attributes = termios.tcgetattr(master)
    assert not attributes[3] & (termios.ICANON | termios.ECHO), 'modal disabled outer raw mode'
    mark = len(transcript)
    send(b'\x03')
    until(prompt, mark)

    command('lcd ' + shlex.quote(str(root / 'empty')))
    open_selector('put ', 'local')
    send(b' \x1b[B')
    assert close_selector(b'\r') == 'put '
    mark = len(transcript)
    send(b'\x03')
    until(prompt, mark)
    (root / 'empty').rmdir()
    mark = len(transcript)
    send('put \x13')
    until(rb'file selector: ', mark)
    assert line(mark) == 'put '
    mark = len(transcript)
    send(b'\x03')
    until(prompt, mark)
    assert not list((root / 'tmp').glob('xfer-directory-*')), 'snapshot files leaked'
    send('detach\r')
    process.wait(timeout=5)
    assert process.returncode == 0
    restored = termios.tcgetattr(master)
    assert restored[3] & (termios.ICANON | termios.ECHO) == original[3] & (termios.ICANON | termios.ECHO)
    print('Ctrl-S PTY: local/remote, quoting, Unicode, literal globs, caret, append, live regexp apply/revert/errors, hidden entries, 2505 entries, j/k/arrows/control paging, sorting/sizes/totals, resize, empty/error, cleanup and terminal restoration PASS')
finally:
    signal.alarm(0)
    if process and process.poll() is None:
        process.kill()
        process.wait()
    # Only signal task-owned daemon process groups, verified by executable and
    # this fixture's unique runtime environment; never broad name-based kills.
    for metadata in (root / 'run').glob('xfer/instances/*.meta'):
        try:
            pid = json.loads(metadata.read_text())['pid']
            owned = ('XDG_RUNTIME_DIR=' + str(root / 'run')).encode() + b'\0'
            if pathlib.Path('/proc/%d/exe' % pid).resolve() == pathlib.Path(binary).resolve() and owned in pathlib.Path('/proc/%d/environ' % pid).read_bytes():
                os.killpg(pid, signal.SIGTERM)
        except (OSError, ValueError, KeyError):
            pass
    if master is not None:
        os.close(master)
