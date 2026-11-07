#!/bin/sh
# Build and package a FreeBSD release from the repository root.
set -eu

if [ "$(uname -s)" != FreeBSD ]; then
    echo 'FreeBSD release packaging must run on FreeBSD' >&2
    exit 1
fi
case "$(uname -m)" in
    amd64|x86_64) arch=x86_64 ;;
    *) echo 'Unsupported FreeBSD architecture' >&2; exit 1 ;;
esac
tag=${1:?usage: package-freebsd.sh TAG}
case "$tag" in
    *[!A-Za-z0-9._-]*) echo 'Invalid release tag' >&2; exit 1 ;;
esac

workspace=$(pwd)
workdir=$(mktemp -d "${TMPDIR:-/tmp}/xfer-freebsd-release.XXXXXXXX")
trap 'rm -rf "$workdir"' EXIT
trap 'exit 1' HUP INT TERM
export CARGO_TARGET_DIR="$workdir/target"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
cargo build --release --locked --bin xfer

name="xfer-${tag}-freebsd-${arch}"
mkdir -p "$workdir/$name" "$workspace/dist"
cp "$CARGO_TARGET_DIR/release/xfer" README.md LICENSE "$workdir/$name/"
gzip -n -c docs/xfer.1 > "$workdir/$name/xfer.1.gz"
tar -C "$workdir" -czf "$workspace/dist/$name.tar.gz" "$name"
printf 'Created %s\n' "$workspace/dist/$name.tar.gz"
