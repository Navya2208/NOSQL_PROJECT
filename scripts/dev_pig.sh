#!/usr/bin/env bash
# Installs Apache Pig 0.18.0 (Hadoop 3 build) into work/pig for the Pig pipeline.
# Downloads about 492 MB from downloads.apache.org and checks Apache's published
# checksum (SHA-512, or MD5 when that is all Apache provides) before unpacking. Deleting work/pig removes it completely.
#
# Usage: scripts/dev_pig.sh install | status
set -euo pipefail

VERSION="${PIG_VERSION:-0.18.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/work/pig"
BASE="https://downloads.apache.org/pig/pig-$VERSION"

case "${1:-}" in
  install)
    if [ -x "$DEST/bin/pig" ]; then
      echo "Pig already installed in work/pig"
      exit 0
    fi
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    # Fetch Apache's checksum first: SHA-512 if published, otherwise MD5 (Pig 0.18.0 has only MD5).
    if curl -fsSL -o "$tmp/pig.sum" "$BASE/pig-$VERSION.tar.gz.sha512" 2>/dev/null; then
      algo=sha512; len=128
    elif curl -fsSL -o "$tmp/pig.sum" "$BASE/pig-$VERSION.tar.gz.md5"; then
      algo=md5; len=32
    else
      echo "No checksum published for pig-$VERSION; not installing an unchecked download." >&2
      exit 1
    fi
    expected="$(tr -d ' \n\r' < "$tmp/pig.sum" | sed 's/.*://' | grep -oE "[0-9A-Fa-f]{$len}" | tr 'A-F' 'a-f' | head -1)"
    echo "Downloading pig-$VERSION.tar.gz (about 492 MB) from $BASE"
    curl -fL --retry 3 -o "$tmp/pig.tar.gz" "$BASE/pig-$VERSION.tar.gz"
    if [ "$algo" = sha512 ]; then
      actual="$(shasum -a 512 "$tmp/pig.tar.gz" | awk '{print $1}')"
    else
      actual="$(md5 -q "$tmp/pig.tar.gz" 2>/dev/null || md5sum "$tmp/pig.tar.gz" | awk '{print $1}')"
    fi
    if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
      echo "Checksum ($algo) mismatch: the download is incomplete or altered. Nothing was installed." >&2
      exit 1
    fi
    echo "Checksum ($algo) OK"
    mkdir -p "$tmp/unpacked" "$ROOT/work"
    tar -xzf "$tmp/pig.tar.gz" -C "$tmp/unpacked"
    rm -rf "$DEST"
    mv "$tmp/unpacked"/pig-* "$DEST"
    echo "Installed Pig $VERSION in work/pig"
    ;;
  status)
    if [ -x "$DEST/bin/pig" ]; then echo "Installed: $DEST"; else echo "Not installed"; fi
    ;;
  *)
    echo "Usage: $0 install | status" >&2
    exit 1
    ;;
esac
