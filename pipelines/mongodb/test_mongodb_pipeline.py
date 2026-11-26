"""Checks the MongoDB pipeline against the test-only reference pipeline.

Needs a running MongoDB (ETL_MONGO_URI, default mongodb://localhost:27017);
skips if none is reachable. Does not need PostgreSQL.

    .venv/bin/pytest pipelines/mongodb -v
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from etl import config
from etl.batching import write_batches

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests"))

from conftest import SAMPLE_AUG, SAMPLE_JUL  # noqa: E402
from reference_pipeline import ReferencePipeline  # noqa: E402

from pipelines.mongodb.pipeline import MongoDBPipeline  # noqa: E402

# Lines that stress regex and decoding edge cases (Latin-1 bytes, tabs,
# non-breaking spaces, stray quotes, carriage returns, empty lines).
EDGE_LINES = [
    b'h1 - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 200 6245',
    b'h1 - - [01/Jul/1995:00:00:01 -0400] "GET /a\xa0b HTTP/1.0" 404 -',
    b'h2 - - [01/Jul/1995:00:00:02 -0400] "GET\xa0/x HTTP/1.0" 400 0',
    b'h3 - - [01/Jul/1995:00:00:03 -0400] "GET /y\x85 HTTP/1.0" 404 0',
    b'h\xe9 - - [01/Jul/1995:00:00:03 -0400] "GET /caf\xe9 HTTP/1.0" 404 0',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "\x80\x81\xff" 400 0',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "GET\t/tab\tHTTP/1.0" 200 1',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "  GET /sp  HTTP/1.0  " 200 1',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "GET /q"uote HTTP/1.0" 200 1',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "GET /a HTTP/1.0 extra" 200 1',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "GET /p HTTP/*" 200 1',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] " " 400 0',
    b'h4 - - [01/Jul/1995:00:00:04 -0400] "GET" 400 0',
    b'h4 - - [01/Jul/1995:23:59:59 -0400] "GET /big HTTP/1.0" 200 99999999999',
    b'h4 - - [31/Dec/1995:23:59:59 +0000] "POST /z HTTP/1.0" 599 5',
    b'h4 - - [32/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1',
    b'h4 - - [01/Jul/1995:24:00:00 -0400] "GET / HTTP/1.0" 200 1',
    b'h4 - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 2000 1',
    b'h4 - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1 ',
    b'h4 - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1\r\r',
    b'h4 - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1\r',
    b'',
    b'\r',
    b'h4  - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1',
    b'h\xa0x - - [01/Jul/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1',
    b'h4 - - [01/Jul/1995:00:00:00 -0400] "GET /a b c" 500 7',
]


@pytest.fixture(scope="module")
def mongo_available():
    try:
        MongoClient(config.mongo_uri(), serverSelectionTimeoutMS=2000).admin.command("ping")
    except PyMongoError as exc:
        pytest.skip(f"MongoDB not reachable at {config.mongo_uri()}: {exc}")


def _run(pipe, inputs, batch_size, work_dir):
    """Drives a pipeline the way etl/controller.py does, without PostgreSQL."""
    run_id = f"test-{uuid.uuid4().hex[:8]}"
    pipe.setup(run_id)
    try:
        batches, per_batch = [], {}
        for batch in write_batches(inputs, batch_size, work_dir / pipe.name):
            res = pipe.process_batch(run_id, batch.batch_id, str(batch.path))
            assert res.record_count == batch.record_count
            batches.append(res)
            per_batch[batch.batch_id] = pipe.run_queries(run_id, batch.batch_id)
        return batches, per_batch, pipe.run_queries(run_id)
    finally:
        pipe.cleanup(run_id)


@pytest.mark.parametrize("batch_size", [1, 3, 1000])
def test_matches_reference(mongo_available, tmp_path, batch_size):
    jul, aug, edge = tmp_path / "jul.log", tmp_path / "aug.log", tmp_path / "edge.log"
    jul.write_bytes(SAMPLE_JUL)
    aug.write_bytes(SAMPLE_AUG)
    edge.write_bytes(b"\n".join(EDGE_LINES) + b"\n")
    inputs = [jul, aug, edge]

    ref = _run(ReferencePipeline(), inputs, batch_size, tmp_path)
    mongo = _run(MongoDBPipeline(), inputs, batch_size, tmp_path)

    assert mongo[0] == ref[0], "per-batch record / parsed / malformed counts differ"
    assert mongo[1] == ref[1], "per-batch Q1-Q3 results differ"
    assert mongo[2] == ref[2], "whole-run Q1-Q3 results differ"
