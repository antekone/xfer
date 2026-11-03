#!/usr/bin/env python3
"""Add a consistent copyright and license header to Rust files under src/."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = ROOT / "src"

HEADER = """/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */
"""

# Recognize the previously proposed wording without removing unrelated comments.
OLD_HEADER = """/*
 * This file is a part of xfer.
 * xfer (c) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * License: GPL-3.0
 */
"""


def add_header(data: bytes) -> bytes:
    """Preserve a UTF-8 BOM, Rust script shebang, and existing line endings."""
    prefix = b""
    if data.startswith(b"\xef\xbb\xbf"):
        prefix, data = data[:3], data[3:]
    if data.startswith(b"#!") and not data.startswith(b"#!["):
        end = data.find(b"\n")
        if end < 0:
            # A shebang must end before the comment can be parsed as Rust.
            return prefix + data + b"\n" + HEADER.encode() + b"\n"
        prefix += data[: end + 1]
        data = data[end + 1 :]

    newline = b"\r\n" if b"\r\n" in data else b"\n"
    header = HEADER.encode().replace(b"\n", newline)
    old_header = OLD_HEADER.encode().replace(b"\n", newline)
    if data.startswith(header):
        return prefix + data
    if data.startswith(old_header):
        return prefix + header + data[len(old_header) :]
    return prefix + header + newline + data


def main() -> None:
    if not SOURCE_DIR.is_dir():
        raise SystemExit(f"source directory does not exist: {SOURCE_DIR}")
    changed = 0
    for path in sorted(SOURCE_DIR.rglob("*.rs")):
        # Do not follow a file symlink outside the source tree.
        if path.is_symlink() or not path.is_file():
            continue
        original = path.read_bytes()
        updated = add_header(original)
        if updated != original:
            path.write_bytes(updated)
            print(path.relative_to(ROOT))
            changed += 1
    print(f"Updated {changed} Rust file(s).")


if __name__ == "__main__":
    main()
