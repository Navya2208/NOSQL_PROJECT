"""Phase 2 benchmark: runs every pipeline the same way and records the results.

Owner: Navya (benchmark machine). Uses only the shared tool's public pieces
(etl.controller, etl.compare, etl.verify); changes nothing in etl/ or tests/.

For each batch size it:
  1. runs the test-only reference pipeline once (the trusted answer);
  2. runs each chosen pipeline `--repeats` times;
  3. checks every pipeline run with `etl compare` against the reference and,
     when MongoDB is in the list, against MongoDB; and with `etl verify`;
  4. writes one CSV row per run plus a Markdown summary table.

A pipeline that fails (not built yet, engine not running, results differ)
is recorded as failed and the benchmark carries on with the next one.

Examples (from the repository root):

    .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb --batch-sizes 100000 500000 1000000
    .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb mapreduce pig hive --batch-sizes 500000 --repeats 3
    .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb --batch-sizes 50 --input tests/golden/sample.log

Results go to benchmark/results/benchmark_<timestamp>.csv and .md.
"""
from __future__ import annotations

import argparse
import csv
import platform
import statistics
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from etl import compare, config, controller, db, verify  # noqa: E402
from etl.report import fetch_run  # noqa: E402
from reference_pipeline import ReferencePipeline  # noqa: E402

FIELDS = [
    "pipeline", "batch_size", "repeat", "run_id", "status", "runtime_seconds",
    "total_records", "parsed_records", "malformed_records", "batch_count", "avg_batch_size",
    "vs_reference", "vs_mongodb", "verify", "error",
]


def _quiet(_message: str) -> None:
    pass


def _run(pipeline_name, batch_size, inputs, log):
    """Runs one pipeline through the real controller. Returns (run_id or None, error text)."""
    try:
        if pipeline_name == "reference":
            run_id = controller.run(batch_size=batch_size, inputs=inputs,
                                    pipeline=ReferencePipeline(), log=log)
        else:
            run_id = controller.run(pipeline_name, batch_size, inputs=inputs, log=log)
        return run_id, ""
    except Exception as exc:  # record and carry on with the next pipeline
        return None, f"{type(exc).__name__}: {exc}".splitlines()[0][:300]


def _check(run_a, run_b):
    if not run_a or not run_b:
        return ""
    try:
        identical, _ = compare.compare(run_a, run_b)
        return "IDENTICAL" if identical else "DIFFERENT"
    except Exception as exc:
        return f"error: {exc}"


def _verify(run_id):
    if not run_id:
        return ""
    try:
        ok, _ = verify.verify(run_id)
        return "all batches match" if ok else "MISMATCH"
    except Exception as exc:
        return f"error: {exc}"


def _row(pipeline_name, batch_size, repeat, run_id, error):
    row = dict.fromkeys(FIELDS, "")
    row.update(pipeline=pipeline_name, batch_size=batch_size, repeat=repeat,
               run_id=run_id or "", error=error)
    if run_id:
        with db.connect() as conn:
            run = fetch_run(conn, run_id)
        for key in ("status", "runtime_seconds", "total_records", "parsed_records",
                    "malformed_records", "batch_count", "avg_batch_size"):
            row[key] = run[key]
    else:
        row["status"] = "failed"
    return row


def _summary(rows, args, started):
    lines = [
        "# Benchmark results", "",
        f"- Started: {started:%Y-%m-%d %H:%M:%S}",
        f"- Machine: {platform.node()} ({platform.system()} {platform.release()}, {platform.machine()})",
        f"- Python: {platform.python_version()}",
        f"- Input: {', '.join(str(p) for p in (args.input or config.default_inputs()))}",
        f"- Repeats per pipeline: {args.repeats}", "",
        "| Pipeline | Batch size | Runs OK | Median runtime (s) | Min (s) | Max (s) "
        "| Records | Malformed | Batches | vs reference | vs MongoDB | verify |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    groups = {}
    for r in rows:
        groups.setdefault((r["pipeline"], r["batch_size"]), []).append(r)
    for (name, size), group in groups.items():
        ok = [r for r in group if r["status"] == "succeeded"]
        times = [float(r["runtime_seconds"]) for r in ok]
        last = ok[-1] if ok else group[-1]

        def agg(key):
            values = sorted({r[key] for r in group if r[key]})
            return ", ".join(values) if values else "-"

        lines.append(
            f"| {name} | {size:,} | {len(ok)}/{len(group)} | "
            f"{statistics.median(times):.2f} | {min(times):.2f} | {max(times):.2f} | "
            if times else f"| {name} | {size:,} | 0/{len(group)} | - | - | - | ")
        lines[-1] += (f"{last['total_records'] or '-'} | {last['malformed_records'] if ok else '-'} | "
                      f"{last['batch_count'] or '-'} | {agg('vs_reference')} | {agg('vs_mongodb')} | "
                      f"{agg('verify')} |")
    errors = [r for r in rows if r["error"]]
    if errors:
        lines += ["", "## Failed runs", ""]
        lines += [f"- {r['pipeline']}, batch size {r['batch_size']:,}, repeat {r['repeat']}: {r['error']}"
                  for r in errors]
    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pipelines", nargs="+", required=True, choices=config.PIPELINE_NAMES)
    p.add_argument("--batch-sizes", nargs="+", type=int, required=True, metavar="N")
    p.add_argument("--repeats", type=int, default=1, help="runs per pipeline and batch size (default 1)")
    p.add_argument("--input", nargs="+", metavar="FILE",
                   help="input files (default: the two NASA files in data/)")
    p.add_argument("--out", type=Path, default=ROOT / "benchmark" / "results")
    p.add_argument("--verbose", action="store_true", help="show the controller's per-batch output")
    args = p.parse_args()

    log = print if args.verbose else _quiet
    started = datetime.now()
    args.out.mkdir(parents=True, exist_ok=True)
    stem = args.out / f"benchmark_{started:%Y%m%d-%H%M%S}"
    rows = []

    # MongoDB first, so the other pipelines can be compared with it.
    order = sorted(args.pipelines, key=lambda n: n != "mongodb")

    for size in args.batch_sizes:
        print(f"\n== batch size {size:,} ==")
        ref_id, err = _run("reference", size, args.input, log)
        print(f"  reference: {ref_id or 'FAILED ' + err}")
        rows.append(_row("reference", size, 1, ref_id, err))
        mongo_id = None
        for name in order:
            for repeat in range(1, args.repeats + 1):
                run_id, err = _run(name, size, args.input, log)
                row = _row(name, size, repeat, run_id, err)
                row["vs_reference"] = _check(ref_id, run_id)
                if name == "mongodb" and run_id and mongo_id is None:
                    mongo_id = run_id
                elif name != "mongodb":
                    row["vs_mongodb"] = _check(mongo_id, run_id)
                row["verify"] = _verify(run_id)
                rows.append(row)
                runtime = f"{float(row['runtime_seconds']):.2f} s" if run_id else "FAILED"
                print(f"  {name} #{repeat}: {runtime}  vs reference: {row['vs_reference'] or '-'}"
                      f"  vs mongodb: {row['vs_mongodb'] or '-'}  {err}")
                # Keep the CSV current, so a crash later still leaves the finished rows.
                with open(f"{stem}.csv", "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=FIELDS)
                    writer.writeheader()
                    writer.writerows(rows)

    Path(f"{stem}.md").write_text(_summary(rows, args, started))
    with open(f"{stem}.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nResults: {stem}.csv\nSummary: {stem}.md")
    return 0 if all(r["status"] == "succeeded" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
