"""Checks that two runs produced identical results (SPEC.md section 9).

Compares record totals, the batch slices (line ranges and hashes), and every
Q1-Q3 row in both scopes: per batch (batch_id 1..n) and whole run (batch_id 0).
"""
from __future__ import annotations

from pathlib import Path

from etl import db
from etl.report import fetch_run
from etl.textfmt import table

TABLES = [
    ("Q1", ["batch_id", "log_date", "status_code", "request_count", "total_bytes"],
     "SELECT batch_id, log_date::text, status_code, request_count, total_bytes "
     "FROM q1_daily_traffic WHERE run_id = %s"),
    ("Q2", ["batch_id", "resource_path", "request_count", "total_bytes", "distinct_host_count"],
     "SELECT batch_id, resource_path, request_count, total_bytes, distinct_host_count "
     "FROM q2_top_resources WHERE run_id = %s"),
    ("Q3", ["batch_id", "log_date", "log_hour", "error_request_count", "total_request_count",
            "error_rate", "distinct_error_hosts"],
     "SELECT batch_id, log_date::text, log_hour, error_request_count, total_request_count, error_rate, "
     "distinct_error_hosts FROM q3_hourly_errors WHERE run_id = %s"),
]
TOTALS = ["total_records", "parsed_records", "malformed_records", "batch_count"]
BATCH_SQL = ("SELECT batch_id, source_file, first_line, last_line, record_count, parsed_count, "
             "malformed_count, sha256 FROM batches WHERE run_id = %s")
MAX_DIFF_ROWS = 10


def _batch_rows(conn, run_id):
    return {(b, Path(src).name, first, last, n, p, m, sha)
            for b, src, first, last, n, p, m, sha in conn.execute(BATCH_SQL, (run_id,)).fetchall()}


def compare(run_a: str, run_b: str, dsn: str | None = None) -> tuple[bool, str]:
    """Returns (identical, printable report)."""
    with db.connect(dsn) as conn:
        runs = {}
        for run_id in (run_a, run_b):
            run = fetch_run(conn, run_id)
            if run is None:
                raise ValueError(f"no run with ID {run_id!r}; list runs with: etl runs")
            if run["status"] != "succeeded":
                raise ValueError(f"run {run_id} has status {run['status']}; only successful runs can be compared")
            runs[run_id] = run
        a, b = runs[run_a], runs[run_b]

        out = [f"Comparing {run_a} ({a['pipeline']}) with {run_b} ({b['pipeline']})", ""]
        warnings = []
        if a["batch_size"] != b["batch_size"]:
            warnings.append(f"batch sizes differ ({a['batch_size']:,} vs {b['batch_size']:,}), "
                            "so per-batch results cannot match")
        if a["input_files"] != b["input_files"]:
            warnings.append("input files differ")

        summary, details, identical = [], [], True

        def add(check, count_a, count_b, same, label_rows=None, headers=None):
            nonlocal identical
            identical &= same
            summary.append((check, count_a, count_b, "identical" if same else "DIFFERENT"))
            for label, rows in label_rows or []:
                if rows:
                    details.extend(["", f"{label} ({len(rows)} rows)", table(headers, rows[:MAX_DIFF_ROWS])])
                    if len(rows) > MAX_DIFF_ROWS:
                        details.append(f"  ... {len(rows) - MAX_DIFF_ROWS} more")

        for key in TOTALS:
            add(key.replace("_", " "), a[key], b[key], a[key] == b[key])

        ba, bb = _batch_rows(conn, run_a), _batch_rows(conn, run_b)
        add("batch slices + hashes", len(ba), len(bb), ba == bb,
            [(f"batches only in {run_a}", sorted(ba - bb)), (f"batches only in {run_b}", sorted(bb - ba))],
            ["batch_id", "source", "first", "last", "records", "parsed", "malformed", "sha256"])

        for code, headers, sql in TABLES:
            rows_a = set(conn.execute(sql, (run_a,)).fetchall())
            rows_b = set(conn.execute(sql, (run_b,)).fetchall())
            for scope, keep in (("whole run", lambda r: r[0] == 0), ("per batch", lambda r: r[0] != 0)):
                sa = {r for r in rows_a if keep(r)}
                sb = {r for r in rows_b if keep(r)}
                add(f"{code} {scope} rows", len(sa), len(sb), sa == sb,
                    [(f"{code} {scope}: only in {run_a}", sorted(sa - sb)),
                     (f"{code} {scope}: only in {run_b}", sorted(sb - sa))], headers)

        out.append(table(["check", run_a, run_b, "result"], summary))
        for w in warnings:
            out.append(f"  warning: {w}")
        out += details
        out += ["", "Result: IDENTICAL" if identical else "Result: DIFFERENT"]
        return identical, "\n".join(out)
