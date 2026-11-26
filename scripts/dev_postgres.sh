#!/usr/bin/env bash
# Private PostgreSQL for development.
#
# Creates a separate database cluster in work/pgdata that listens only on
# localhost:5433, with user "etl", database "etl" and no password (trust
# authentication). It does not touch any other PostgreSQL on the machine.
#
# Usage: scripts/dev_postgres.sh start | stop | status
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/work/pgdata"
LOG="$ROOT/work/pgdata.log"
PORT="${ETL_PG_PORT:-5433}"

find_pg_bin() {
  if [ -n "${PG_BIN:-}" ]; then echo "$PG_BIN"; return; fi
  if command -v pg_ctl >/dev/null 2>&1; then dirname "$(command -v pg_ctl)"; return; fi
  for dir in /Library/PostgreSQL/*/bin /opt/homebrew/opt/postgresql@*/bin /usr/lib/postgresql/*/bin; do
    if [ -x "$dir/pg_ctl" ]; then echo "$dir"; return; fi
  done
  echo "PostgreSQL binaries not found. Set PG_BIN to the folder containing pg_ctl." >&2
  exit 1
}
BIN="$(find_pg_bin)"

case "${1:-}" in
  start)
    mkdir -p "$ROOT/work"
    if [ ! -f "$DATA/PG_VERSION" ]; then
      "$BIN/initdb" -D "$DATA" -U etl --auth=trust --encoding=UTF8 --locale=C >/dev/null
    fi
    if "$BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
      echo "Already running on localhost:$PORT"
    else
      "$BIN/pg_ctl" -D "$DATA" -l "$LOG" -w \
        -o "-p $PORT -c listen_addresses=localhost -k /tmp" start >/dev/null
      echo "Started on localhost:$PORT (log: work/pgdata.log)"
    fi
    if ! "$BIN/psql" -h localhost -p "$PORT" -U etl -d postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname = 'etl'" | grep -q 1; then
      "$BIN/createdb" -h localhost -p "$PORT" -U etl etl
      echo "Created database etl"
    fi
    ;;
  stop)
    "$BIN/pg_ctl" -D "$DATA" -m fast stop
    ;;
  status)
    "$BIN/pg_ctl" -D "$DATA" status || true
    ;;
  *)
    echo "Usage: $0 start | stop | status" >&2
    exit 1
    ;;
esac
