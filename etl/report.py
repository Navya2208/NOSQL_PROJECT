"""Reads one run from PostgreSQL and prints its results with execution metadata.

By default it shows the whole-run results (batch_id 0), the final answer.
With batch=k it shows Q1-Q3 computed over only batch k's records.
Reads only PostgreSQL, never a pipeline, and is not part of the measured runtime.
"""
from __future__ import annotations

from pathlib import Path

from etl import db
from etl.loader import WHOLE_RUN
from etl.textfmt import fmt_value, key_values, table

QUERIES = [
    ("Q1", "Daily traffic summary",
     "SELECT log_date, status_code, request_count, total_bytes, executed_at "
     "FROM q1_daily_traffic WHERE run_id = %s AND batch_id = %s ORDER BY log_date, status_code",
     ["log_date", "status_code", "request_count", "total_bytes"]),
    ("Q2", "Top requested resources",
     "SELECT resource_path, request_count, total_bytes, distinct_host_count, executed_at "
     "FROM q2_top_resources WHERE run_id = %s AND batch_id = %s "
     "ORDER BY request_count DESC, resource_path",
     ["resource_path", "request_count", "total_bytes", "distinct_host_count"]),
    ("Q3", "Hourly error analysis",
     "SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, "
     "distinct_error_hosts, executed_at "
     "FROM q3_hourly_errors WHERE run_id = %s AND batch_id = %s ORDER BY log_date, log_hour",
     ["log_date", "log_hour", "error_request_count", "total_request_count", "error_rate",
      "distinct_error_hosts"]),
]

RUN_COLUMNS = ("run_id, pipeline, status, batch_size, input_files, started_at, finished_at, "
               "runtime_seconds, total_records, parsed_records, malformed_records, batch_count, "
               "avg_batch_size, error_message")


def latest_run_id(conn) -> str | None:
    row = conn.execute(
        "SELECT run_id FROM runs WHERE status = 'succeeded' ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def fetch_run(conn, run_id: str) -> dict | None:
    cur = conn.execute(f"SELECT {RUN_COLUMNS} FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip([d.name for d in cur.description], row))


def render(conn, run_id: str, max_rows: int = 25, batch: int = WHOLE_RUN) -> str:
    run = fetch_run(conn, run_id)
    if run is None:
        raise ValueError(f"no run with ID {run_id!r}; list runs with: etl runs")

    runtime = f"{run['runtime_seconds']:.3f} s" if run["runtime_seconds"] is not None else "-"
    avg = f"{run['avg_batch_size']:,.2f}" if run["avg_batch_size"] is not None else "-"
    out = [f"Run {run['run_id']}", key_values([
        ("Pipeline", run["pipeline"]),
        ("Status", run["status"]),
        ("Input files", run["input_files"]),
        ("Started", run["started_at"]),
        ("Runtime", runtime),
        ("Batch size", run["batch_size"]),
        ("Batches", run["batch_count"]),
        ("Average batch size", avg),
        ("Total records", run["total_records"]),
        ("Parsed records", run["parsed_records"]),
        ("Malformed records", run["malformed_records"]),
    ])]
    if run["error_message"]:
        out.append(f"  Error               {run['error_message']}")
    if run["status"] != "succeeded":
        return "\n".join(out)

    batches = conn.execute(
        "SELECT batch_id, source_file, first_line, last_line, record_count, parsed_count, "
        "malformed_count, sha256 FROM batches WHERE run_id = %s ORDER BY batch_id", (run_id,)).fetchall()
    batch_rows = [(b, Path(src).name, f"{first:,}-{last:,}", n, p, m, sha[:12])
                  for b, src, first, last, n, p, m, sha in batches]
    out += ["", "Batches", table(
        ["batch_id", "source", "lines", "records", "parsed", "malformed", "sha256"], batch_rows)]

    if batch == WHOLE_RUN:
        scope = f"0 (whole run: all parsed records of batches 1-{run['batch_count']})"
    else:
        match = [r for r in batch_rows if r[0] == batch]
        if not match:
            raise ValueError(f"run {run_id} has no batch {batch}; it has batches 1-{run['batch_count']}")
        scope = f"{batch} (only this batch: {match[0][1]} lines {match[0][2]})"

    for code, title, sql, headers in QUERIES:
        rows = conn.execute(sql, (run_id, batch)).fetchall()
        executed_at = rows[0][-1] if rows else None
        out += ["", f"{code} {title}", key_values([
            ("Pipeline", run["pipeline"]),
            ("Query", f"{code} {title}"),
            ("Runtime", runtime),
            ("Batch ID", scope),
            ("Batch size", run["batch_size"]),
            ("Average batch size", avg),
            ("Executed at", fmt_value(executed_at)),
            ("Rows", len(rows)),
        ])]
        data = [r[:-1] for r in rows]
        shown = data if max_rows <= 0 else data[:max_rows]
        out += ["", table(headers, shown)]
        if len(shown) < len(data):
            out.append(f"  ... {len(data) - len(shown):,} more rows (show all with --rows 0)")
    return "\n".join(out)


def list_runs(conn, limit: int = 20) -> str:
    rows = conn.execute(
        "SELECT run_id, pipeline, status, batch_size, batch_count, total_records, runtime_seconds "
        "FROM runs ORDER BY started_at DESC LIMIT %s", (limit,)).fetchall()
    if not rows:
        return "No runs yet."
    return table(["run_id", "pipeline", "status", "batch_size", "batches", "records", "runtime_s"],
                 rows)


def report(run_id: str | None = None, latest: bool = False, max_rows: int = 25,
           batch: int = WHOLE_RUN, dsn: str | None = None) -> str:
    with db.connect(dsn) as conn:
        if latest:
            run_id = latest_run_id(conn)
            if run_id is None:
                raise ValueError("no successful runs yet")
        return render(conn, run_id, max_rows, batch)
