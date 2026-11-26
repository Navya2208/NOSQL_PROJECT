#!/usr/bin/env bash
# Private MongoDB for development, from MongoDB's official build. No Homebrew or Docker.
#
# The first start downloads MongoDB Community (about 82 MB, from fastdl.mongodb.org)
# into work/mongodb. Data lives in work/mongodata. It listens only on localhost:27017.
# Deleting work/mongodb and work/mongodata removes it completely.
#
# Usage: scripts/dev_mongodb.sh start | stop | status
set -euo pipefail

VERSION="${MONGODB_VERSION:-8.0.38}"
PORT="${ETL_MONGO_PORT:-27017}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOME_DIR="$ROOT/work/mongodb"
DATA="$ROOT/work/mongodata"
LOG="$ROOT/work/mongodb.log"
PIDFILE="$ROOT/work/mongodb.pid"
MONGOD="$HOME_DIR/bin/mongod"

download_url() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os/$arch" in
    Darwin/arm64)  echo "https://fastdl.mongodb.org/osx/mongodb-macos-arm64-$VERSION.tgz" ;;
    Darwin/x86_64) echo "https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-$VERSION.tgz" ;;
    *)
      echo "Automatic download supports macOS only. On $os/$arch, install MongoDB $VERSION from" >&2
      echo "https://www.mongodb.com/try/download/community and make sure it listens on localhost:$PORT." >&2
      exit 1 ;;
  esac
}

install() {
  local url tmp
  url="$(download_url)"
  tmp="$(mktemp -d)"
  echo "Downloading MongoDB $VERSION from $url"
  curl -fL --retry 3 -o "$tmp/mongodb.tgz" "$url"
  mkdir -p "$tmp/unpacked"
  tar -xzf "$tmp/mongodb.tgz" -C "$tmp/unpacked"
  rm -rf "$HOME_DIR"
  mkdir -p "$ROOT/work"
  mv "$tmp/unpacked"/mongodb-* "$HOME_DIR"
  rm -rf "$tmp"
  echo "Installed in work/mongodb"
}

running() {
  [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

case "${1:-}" in
  start)
    [ -x "$MONGOD" ] || install
    if running; then
      echo "Already running on localhost:$PORT (pid $(cat "$PIDFILE"))"
      exit 0
    fi
    if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "Port $PORT is already in use by another program. Stop it or set ETL_MONGO_PORT." >&2
      exit 1
    fi
    mkdir -p "$DATA"
    "$MONGOD" --dbpath "$DATA" --bind_ip 127.0.0.1 --port "$PORT" \
      --logpath "$LOG" --logappend --pidfilepath "$PIDFILE" --fork >/dev/null
    echo "Started MongoDB $VERSION on localhost:$PORT (log: work/mongodb.log)"
    ;;
  stop)
    if running; then
      kill "$(cat "$PIDFILE")"
      for _ in $(seq 1 30); do running || break; sleep 1; done
      rm -f "$PIDFILE"
      echo "Stopped"
    else
      echo "Not running"
    fi
    ;;
  status)
    if running; then echo "Running on localhost:$PORT (pid $(cat "$PIDFILE"))"; else echo "Not running"; fi
    ;;
  *)
    echo "Usage: $0 start | stop | status" >&2
    exit 1
    ;;
esac
