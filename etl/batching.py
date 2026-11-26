"""Cuts the raw input files into batches of N unchanged lines (SPEC.md section 3).

Rules:
  - split only on b"\\n"; no decoding, trimming, cleaning or filtering;
  - bytes are copied exactly, including a file's last line with no newline;
  - each input file is batched on its own, so a batch never joins two files;
  - batch IDs start at 1 and continue across files;
  - each batch records its source file, line range and SHA-256, so anyone can
    re-read the raw file and prove the slice is untouched (etl verify).

The controller never parses, filters or aggregates.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator


@dataclass(frozen=True)
class Batch:
    batch_id: int
    path: Path            # batch file given to the pipeline
    record_count: int
    source_file: Path     # raw input file the lines came from
    first_line: int       # 1-based line numbers within source_file
    last_line: int
    sha256: str           # of the exact bytes in the batch file


def write_batches(inputs: Iterable[Path], batch_size: int, out_dir: Path) -> Iterator[Batch]:
    """Writes batch_00001.log, batch_00002.log, ... into out_dir, yielding each when complete.

    The last batch of each file may hold fewer than batch_size records.
    An empty file produces no batch.
    """
    if batch_size < 1:
        raise ValueError("batch size must be at least 1")
    out_dir.mkdir(parents=True, exist_ok=True)

    batch_id = 0
    out: BinaryIO | None = None
    try:
        for source in inputs:
            source = Path(source)
            with open(source, "rb") as f:
                line_no = 0
                for line in f:  # binary iteration splits on b"\n" only
                    line_no += 1
                    if out is None:
                        batch_id += 1
                        path = out_dir / f"batch_{batch_id:05d}.log"
                        out = open(path, "wb")
                        digest = hashlib.sha256()
                        first, count = line_no, 0
                    out.write(line)
                    digest.update(line)
                    count += 1
                    if count == batch_size:
                        out.close()
                        out = None
                        yield Batch(batch_id, path, count, source, first, line_no, digest.hexdigest())
                if out is not None:  # short last batch of this file
                    out.close()
                    out = None
                    yield Batch(batch_id, path, count, source, first, line_no, digest.hexdigest())
    finally:
        if out is not None:
            out.close()


def hash_line_ranges(source: Path, ranges: list[tuple[int, int]]) -> list[str | None]:
    """SHA-256 of each (first_line, last_line) range of a raw file, in one pass.

    Returns None for a range that runs past the end of the file.
    """
    order = sorted(range(len(ranges)), key=lambda i: ranges[i][0])
    results: list[str | None] = [None] * len(ranges)
    pos = 0
    digest = None
    with open(source, "rb") as f:
        for line_no, line in enumerate(f, start=1):
            while pos < len(order) and line_no > ranges[order[pos]][1]:
                pos += 1
                digest = None
            if pos >= len(order):
                break
            first, last = ranges[order[pos]]
            if line_no < first:
                continue
            if digest is None:
                digest = hashlib.sha256()
            digest.update(line)
            if line_no == last:
                results[order[pos]] = digest.hexdigest()
                digest = None
                pos += 1
    return results
