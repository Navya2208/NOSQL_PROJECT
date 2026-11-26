#!/usr/bin/env bash
# Downloads the two official NASA HTTP log files into data/ and decompresses them.
# Decompression is the only change allowed by the project statement.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/data"
cd "$ROOT/data"

for name in NASA_access_log_Jul95 NASA_access_log_Aug95; do
  if [ -f "$name" ]; then
    echo "$name already present"
    continue
  fi
  curl -fL --retry 3 -o "$name.gz" "https://ita.ee.lbl.gov/traces/$name.gz"
  gunzip "$name.gz"
  echo "$name: $(wc -l < "$name") lines"
done
