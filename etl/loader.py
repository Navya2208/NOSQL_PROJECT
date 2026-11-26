"""Writes run metadata and query results to PostgreSQL (SPEC.md sections 6 and 7)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import psycopg

from etl.pipeline import QueryResults

WHOLE_RUN = 0  # batch_id stored on whole-run result rows


@dataclass(frozen=True)
class BatchRow:
    batch_id: int
    record_count: int
    parsed_count: int
    malformed_count: int
    source_file: str
    first_line: int
    last_line: int
    sha256: str


@dataclass(frozen=True)
class ScopedResults:
    batch_id: int            # 1..n for one batch, WHOLE_RUN for the whole run
    executed_at: datetime    # when this scope's queries finished
    results: QueryResults


def unique_run_id(conn: psycopg.Connection, base: str) -> str:
    candidate, n = base, 2
    while conn.execute("SELECT 1 FROM runs WHERE run_id = %s", (candidate,)).fetchone():
        candidate, n = f"{base}-{n}", n + 1
    return candidate


def create_run(conn, run_id: str, pipeline: str, batch_size: int,
               input_files: str, started_at: datetime) -> None:
    conn.execute(
        """INSERT INTO runs (run_id, pipeline, batch_size, input_files, status, started_at)
           VALUES (%s, %s, %s, %s, 'running', %s)""",
        (run_id, pipeline, batch_size, input_files, started_at),
    )


def write_results(conn, run_id: str, pipeline: str, batches: list[BatchRow],
                  scoped: list[ScopedResults]) -> None:
    """Writes batch rows and every scope's Q1-Q3 rows in one transaction."""
    q1, q2, q3 = [], [], []
    for s in scoped:
        meta = (run_id, pipeline, s.batch_id, s.executed_at)
        q1 += [(*meta, date.fromisoformat(d), st, c, b) for d, st, c, b in s.results.q1]
        q2 += [(*meta, *row) for row in s.results.q2]
        q3 += [(*meta, date.fromisoformat(d), h, e, t, r, x) for d, h, e, t, r, x in s.results.q3]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO batches (run_id, batch_id, record_count, parsed_count, malformed_count,
                                        source_file, first_line, last_line, sha256)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [(run_id, b.batch_id, b.record_count, b.parsed_count, b.malformed_count,
                  b.source_file, b.first_line, b.last_line, b.sha256) for b in batches],
            )
            cur.executemany(
                """INSERT INTO q1_daily_traffic
                   (run_id, pipeline, batch_id, executed_at, log_date, status_code, request_count, total_bytes)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", q1)
            cur.executemany(
                """INSERT INTO q2_top_resources
                   (run_id, pipeline, batch_id, executed_at, resource_path, request_count, total_bytes,
                    distinct_host_count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", q2)
            cur.executemany(
                """INSERT INTO q3_hourly_errors
                   (run_id, pipeline, batch_id, executed_at, log_date, log_hour, error_request_count,
                    total_request_count, error_rate, distinct_error_hosts)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", q3)


def finish_run(conn, run_id: str, finished_at: datetime, runtime_seconds: float,
               batches: list[BatchRow]) -> None:
    total = sum(b.record_count for b in batches)
    parsed = sum(b.parsed_count for b in batches)
    malformed = sum(b.malformed_count for b in batches)
    non_empty = sum(1 for b in batches if b.record_count > 0)
    avg = total / non_empty if non_empty else None
    conn.execute(
        """UPDATE runs SET status = 'succeeded', finished_at = %s, runtime_seconds = %s,
                  total_records = %s, parsed_records = %s, malformed_records = %s,
                  batch_count = %s, avg_batch_size = %s
           WHERE run_id = %s""",
        (finished_at, runtime_seconds, total, parsed, malformed, len(batches), avg, run_id),
    )


def fail_run(conn, run_id: str, message: str) -> None:
    conn.execute(
        """UPDATE runs SET status = 'failed', finished_at = now(), error_message = %s
           WHERE run_id = %s""",
        (message[:2000], run_id),
    )
