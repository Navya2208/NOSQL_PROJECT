"""Plain-Python reference implementation of SPEC.md, for tests only.

It is never registered as a pipeline of the tool, because the project requires
real processing in MongoDB, MapReduce, Pig or Hive. It exists to:
  - test the controller, loader, report and compare without any backend;
  - work out expected results for the golden sample, to check real pipelines against.
"""
from __future__ import annotations

import re
from collections import defaultdict

from etl.pipeline import BatchResult, Pipeline, QueryResults

LINE_RE = re.compile(
    r'^(\S+) \S+ \S+ \[((0[1-9]|[12]\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/'
    r'(\d{4}):([01]\d|2[0-3]):[0-5]\d:[0-5]\d [+-]\d{4})\] "(.*)" (\d{3}) (\d+|-)$',
    re.ASCII)
REQUEST_RE = re.compile(r'^(\S+)\s+(.+?)(?:\s+(HTTP/\S+))?$', re.ASCII)
MONTHS = {m: i for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), start=1)}


def parse_line(raw: bytes) -> dict | None:
    """Returns the nine fields, or None for a malformed line (SPEC.md section 2)."""
    text = raw.decode("latin-1")
    if text.endswith("\n"):
        text = text[:-1]
    if text.endswith("\r"):
        text = text[:-1]
    m = LINE_RE.fullmatch(text)
    if m is None:
        return None
    request = m.group(7).strip(" ")
    r = REQUEST_RE.fullmatch(request)
    method, path, protocol = (r.group(1), r.group(2), r.group(3) or "") if r else (request, "", "")
    return {
        "host": m.group(1),
        "timestamp": m.group(2),
        "log_date": f"{m.group(5)}-{MONTHS[m.group(4)]:02d}-{m.group(3)}",
        "log_hour": int(m.group(6)),
        "http_method": method,
        "resource_path": path,
        "protocol_version": protocol,
        "status_code": int(m.group(8)),
        "bytes_transferred": 0 if m.group(9) == "-" else int(m.group(9)),
    }


class Aggregator:
    """Exact Q1-Q3 state for a set of records. Distinct hosts are kept as real sets,
    so nothing is ever estimated or added up across batches."""

    def __init__(self):
        self.q1 = defaultdict(lambda: [0, 0])
        self.q2 = defaultdict(lambda: [0, 0, set()])
        self.q3 = defaultdict(lambda: [0, 0, set()])

    def add(self, r: dict) -> None:
        a = self.q1[(r["log_date"], r["status_code"])]
        a[0] += 1
        a[1] += r["bytes_transferred"]
        b = self.q2[r["resource_path"]]
        b[0] += 1
        b[1] += r["bytes_transferred"]
        b[2].add(r["host"])
        c = self.q3[(r["log_date"], r["log_hour"])]
        c[1] += 1
        if 400 <= r["status_code"] <= 599:
            c[0] += 1
            c[2].add(r["host"])

    def results(self) -> QueryResults:
        top = sorted(self.q2.items(), key=lambda kv: (-kv[1][0], kv[0]))[:20]
        return QueryResults(
            q1=[(d, s, n, b) for (d, s), (n, b) in sorted(self.q1.items())],
            q2=[(p, n, b, len(h)) for p, (n, b, h) in top],
            q3=[(d, h, e, t, e / t, len(x)) for (d, h), (e, t, x) in sorted(self.q3.items())],
        )


def run_queries(records: list[dict]) -> QueryResults:
    agg = Aggregator()
    for r in records:
        agg.add(r)
    return agg.results()


class ReferencePipeline(Pipeline):
    name = "reference"

    def setup(self, run_id: str) -> None:
        self.whole = Aggregator()                       # every parsed record of the run
        self.per_batch: dict[int, QueryResults] = {}   # results over each batch alone

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        count = malformed = 0
        batch = Aggregator()
        with open(batch_path, "rb") as f:
            for raw in f:
                count += 1
                parsed = parse_line(raw)
                if parsed is None:
                    malformed += 1
                else:
                    batch.add(parsed)
                    self.whole.add(parsed)
        self.per_batch[batch_id] = batch.results()
        return BatchResult(count, count - malformed, malformed)

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        if batch_id is None:
            return self.whole.results()
        return self.per_batch[batch_id]

    def cleanup(self, run_id: str) -> None:
        self.whole = Aggregator()
        self.per_batch = {}
