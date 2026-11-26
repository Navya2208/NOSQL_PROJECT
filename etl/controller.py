"""Runs one complete ETL flow with the selected pipeline (SPEC.md sections 3 and 7).

For each batch: the pipeline parses it, then runs Q1-Q3 over that batch only.
After the last batch: the pipeline runs Q1-Q3 over every parsed record (the
whole-run answer, stored with batch_id 0). Everything is loaded in one transaction.

Runtime clock: starts immediately before the first input file is opened and
stops immediately after that PostgreSQL transaction commits.
"""
from __future__ import annotations

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from etl import config, db, loader
from etl.batching import Batch, write_batches
from etl.loader import WHOLE_RUN, BatchRow, ScopedResults
from etl.pipeline import BatchResult, Pipeline, load_pipeline, validate_results


class BatchMismatch(RuntimeError):
    """The pipeline's record counts do not match the lines the controller sent."""


def _check_batch(batch: Batch, result: BatchResult) -> None:
    if result.record_count != batch.record_count:
        raise BatchMismatch(
            f"batch {batch.batch_id}: controller sent {batch.record_count} lines, "
            f"pipeline reported {result.record_count}")
    if result.parsed_count + result.malformed_count != result.record_count:
        raise BatchMismatch(
            f"batch {batch.batch_id}: parsed {result.parsed_count} + malformed "
            f"{result.malformed_count} != {result.record_count} records")


def run(pipeline_name: str | None = None, batch_size: int = 500_000,
        inputs: Sequence[Path | str] | None = None, dsn: str | None = None,
        work_root: Path | None = None, keep_work: bool = False,
        pipeline: Pipeline | None = None, log: Callable[[str], None] = print) -> str:
    """Runs the full ETL flow and returns the run ID.

    Pass `pipeline` to use an already-built pipeline object (used by tests);
    otherwise `pipeline_name` selects one from the registry.
    """
    input_paths = [Path(p) for p in (inputs or config.default_inputs())]
    missing = [str(p) for p in input_paths if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "input file not found: " + ", ".join(missing)
            + ". Download the dataset with scripts/download_data.sh")
    if batch_size < 1:
        raise ValueError("batch size must be at least 1")

    pipe = pipeline or load_pipeline(pipeline_name)

    with db.connect(dsn) as conn:
        base_id = f"{datetime.now():%Y%m%d-%H%M%S}-{pipe.name}"
        run_id = loader.unique_run_id(conn, base_id)
        loader.create_run(conn, run_id, pipe.name, batch_size,
                          ", ".join(str(p) for p in input_paths), datetime.now(timezone.utc))
        work_dir = Path(work_root or config.work_dir()) / run_id
        log(f"Run {run_id}: pipeline {pipe.name}, batch size {batch_size:,}")

        batches: list[BatchRow] = []
        scoped: list[ScopedResults] = []
        try:
            pipe.setup(run_id)

            start = time.perf_counter()  # clock starts: first input file is opened next
            for batch in write_batches(input_paths, batch_size, work_dir):
                result = pipe.process_batch(run_id, batch.batch_id, str(batch.path))
                _check_batch(batch, result)
                batch_results = pipe.run_queries(run_id, batch.batch_id)
                validate_results(batch_results, f"batch {batch.batch_id}")
                scoped.append(ScopedResults(batch.batch_id, datetime.now(timezone.utc), batch_results))
                batches.append(BatchRow(
                    batch.batch_id, result.record_count, result.parsed_count, result.malformed_count,
                    str(batch.source_file), batch.first_line, batch.last_line, batch.sha256))
                log(f"  batch {batch.batch_id}: {batch.source_file.name} lines "
                    f"{batch.first_line:,}-{batch.last_line:,}, {result.record_count:,} records, "
                    f"{result.malformed_count:,} malformed")

            whole = pipe.run_queries(run_id)
            validate_results(whole, "whole run")
            scoped.append(ScopedResults(WHOLE_RUN, datetime.now(timezone.utc), whole))
            loader.write_results(conn, run_id, pipe.name, batches, scoped)
            runtime = time.perf_counter() - start  # clock stops: results committed
        except BaseException as exc:
            loader.fail_run(conn, run_id, f"{type(exc).__name__}: {exc}")
            log(f"Run {run_id} failed: {type(exc).__name__}: {exc}")
            raise
        finally:
            try:
                pipe.cleanup(run_id)
            except Exception as exc:  # cleanup problems must not hide the real outcome
                log(f"  warning: cleanup failed: {exc}")
            if not keep_work:
                shutil.rmtree(work_dir, ignore_errors=True)

        loader.finish_run(conn, run_id, datetime.now(timezone.utc), runtime, batches)

    total = sum(b.record_count for b in batches)
    log(f"Run {run_id} succeeded: {total:,} records in {len(batches)} batches, "
        f"runtime {runtime:.3f} s")
    return run_id
