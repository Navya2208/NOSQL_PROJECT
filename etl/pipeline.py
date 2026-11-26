"""The interface every pipeline implements (SPEC.md section 4), and result validation.

A pipeline does all parsing and all query work inside its own technology
(MongoDB, Hadoop MapReduce, Pig or Hive). The Python class only starts that
work and reads the results back in the shapes defined here.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from datetime import date

Q1_COLUMNS = ("log_date", "status_code", "request_count", "total_bytes")
Q2_COLUMNS = ("resource_path", "request_count", "total_bytes", "distinct_host_count")
Q3_COLUMNS = (
    "log_date", "log_hour", "error_request_count",
    "total_request_count", "error_rate", "distinct_error_hosts",
)
Q2_LIMIT = 20


@dataclass
class BatchResult:
    record_count: int      # lines received in this batch
    parsed_count: int
    malformed_count: int   # parsed_count + malformed_count == record_count


@dataclass
class QueryResults:
    q1: list[tuple] = field(default_factory=list)  # Q1_COLUMNS
    q2: list[tuple] = field(default_factory=list)  # Q2_COLUMNS, at most 20 rows
    q3: list[tuple] = field(default_factory=list)  # Q3_COLUMNS


class Pipeline:
    """Base class. Each backend in pipelines/<name>/ subclasses it."""

    name: str = ""

    def setup(self, run_id: str) -> None:
        """Create empty working storage for this run (collections, HDFS folders, tables)."""

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        """Load and parse one batch file with this pipeline's technology."""
        raise NotImplementedError

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        """Run Q1, Q2 and Q3 with this pipeline's technology.

        batch_id = k:    over only batch k's parsed records (called right after batch k).
        batch_id = None: over every parsed record of the run (called once, after the last batch).
        Never build the whole-run answer by adding up per-batch results.
        """
        raise NotImplementedError

    def cleanup(self, run_id: str) -> None:
        """Remove working storage. Never touches PostgreSQL."""


REGISTRY = {
    "mongodb": "pipelines.mongodb.pipeline:MongoDBPipeline",
    "mapreduce": "pipelines.mapreduce.pipeline:MapReducePipeline",
    "pig": "pipelines.pig.pipeline:PigPipeline",
    "hive": "pipelines.hive.pipeline:HivePipeline",
}


def load_pipeline(name: str) -> Pipeline:
    if name not in REGISTRY:
        raise ValueError(f"unknown pipeline {name!r}; choose one of {', '.join(REGISTRY)}")
    module_name, class_name = REGISTRY[name].split(":")
    cls = getattr(importlib.import_module(module_name), class_name)
    return cls()


class ResultError(ValueError):
    """A pipeline returned rows that break the shapes or rules in SPEC.md."""


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_date(value) -> bool:
    if not isinstance(value, str) or len(value) != 10:
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def validate_results(results: QueryResults, scope: str = "whole run") -> None:
    """Raises ResultError on the first row that breaks SPEC.md section 5."""

    def fail(query: str, index: int, message: str):
        raise ResultError(f"{scope}, {query} row {index + 1}: {message}")

    seen = set()
    for i, row in enumerate(results.q1):
        if len(row) != len(Q1_COLUMNS):
            fail("Q1", i, f"expected {len(Q1_COLUMNS)} columns {Q1_COLUMNS}, got {len(row)}")
        log_date, status, count, total_bytes = row
        if not _check_date(log_date):
            fail("Q1", i, f"log_date must be a 'YYYY-MM-DD' string, got {log_date!r}")
        if not all(_is_int(v) for v in (status, count, total_bytes)):
            fail("Q1", i, "status_code, request_count and total_bytes must be integers")
        if (log_date, status) in seen:
            fail("Q1", i, f"duplicate group ({log_date}, {status})")
        seen.add((log_date, status))

    if len(results.q2) > Q2_LIMIT:
        raise ResultError(f"{scope}, Q2 returned {len(results.q2)} rows; at most {Q2_LIMIT} allowed")
    seen = set()
    for i, row in enumerate(results.q2):
        if len(row) != len(Q2_COLUMNS):
            fail("Q2", i, f"expected {len(Q2_COLUMNS)} columns {Q2_COLUMNS}, got {len(row)}")
        path, count, total_bytes, hosts = row
        if not isinstance(path, str):
            fail("Q2", i, f"resource_path must be a string (use '' for missing), got {path!r}")
        if not all(_is_int(v) for v in (count, total_bytes, hosts)):
            fail("Q2", i, "request_count, total_bytes and distinct_host_count must be integers")
        if path in seen:
            fail("Q2", i, f"duplicate resource_path {path!r}")
        seen.add(path)

    seen = set()
    for i, row in enumerate(results.q3):
        if len(row) != len(Q3_COLUMNS):
            fail("Q3", i, f"expected {len(Q3_COLUMNS)} columns {Q3_COLUMNS}, got {len(row)}")
        log_date, hour, errors, total, rate, hosts = row
        if not _check_date(log_date):
            fail("Q3", i, f"log_date must be a 'YYYY-MM-DD' string, got {log_date!r}")
        if not all(_is_int(v) for v in (hour, errors, total, hosts)):
            fail("Q3", i, "log_hour and the counts must be integers")
        if not 0 <= hour <= 23:
            fail("Q3", i, f"log_hour must be 0 to 23, got {hour}")
        if total < 1 or not 0 <= errors <= total:
            fail("Q3", i, f"need 0 <= error_request_count <= total_request_count and total >= 1, got {errors}/{total}")
        if not isinstance(rate, float) or rate != errors / total:
            fail("Q3", i, f"error_rate must be the unrounded float {errors}/{total} = {errors / total!r}, got {rate!r}")
        if (log_date, hour) in seen:
            fail("Q3", i, f"duplicate group ({log_date}, {hour})")
        seen.add((log_date, hour))
