"""Proves a run's batches were unchanged slices of the raw files (SPEC.md section 3).

Re-reads each raw input file, hashes the stored line range of every batch, and
checks it against the SHA-256 the controller recorded when it cut the batch.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from etl import db
from etl.batching import hash_line_ranges
from etl.textfmt import table


def verify(run_id: str, dsn: str | None = None) -> tuple[bool, str]:
    with db.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT batch_id, source_file, first_line, last_line, sha256 FROM batches "
            "WHERE run_id = %s ORDER BY batch_id", (run_id,)).fetchall()
    if not rows:
        raise ValueError(f"run {run_id!r} has no stored batches; list runs with: etl runs")

    by_file: dict[str, list] = defaultdict(list)
    for row in rows:
        by_file[row[1]].append(row)

    results, ok = [], True
    for source, batches in by_file.items():
        path = Path(source)
        if not path.is_file():
            for b in batches:
                results.append((b[0], path.name, f"{b[2]:,}-{b[3]:,}", "source file missing"))
            ok = False
            continue
        hashes = hash_line_ranges(path, [(b[2], b[3]) for b in batches])
        for b, actual in zip(batches, hashes):
            match = actual == b[4]
            ok &= match
            results.append((b[0], path.name, f"{b[2]:,}-{b[3]:,}", "match" if match else "MISMATCH"))

    text = "\n".join([
        f"Verifying batches of {run_id} against the raw input files", "",
        table(["batch_id", "source", "lines", "sha256"], sorted(results)), "",
        "Result: every batch is an unchanged slice of its source file" if ok
        else "Result: SOME BATCHES DO NOT MATCH their source files",
    ])
    return ok, text
