"""MongoDB pipeline. Owner: Navya.

Implements etl.pipeline.Pipeline following SPEC.md. Every rule of the spec
runs inside MongoDB:

  raw_lines_<run>    one document per input line, the line exactly as read
                     (Latin-1 decoded, nothing trimmed or filtered)
  parsed_logs_<run>  one document per input line, written by an aggregation
                     that applies the SPEC.md section 2 regexes with $regexFind
                     and flags lines that do not match as malformed

Python here only:
  - frames the batch file into lines (split on "\\n", the record boundary of
    SPEC.md section 1) and inserts each line unchanged, with its batch id and
    line number, into raw_lines;
  - starts the parse and query aggregations and reads their output back into
    the tuples defined in etl/pipeline.py.

Removing the trailing "\\r", regex matching, malformed detection, field
derivation, counting, grouping, distinct hosts, the error rate, sorting and
the top-20 limit all happen in MongoDB aggregation pipelines.

Connection string: etl.config.mongo_uri() (ETL_MONGO_URI).
Database name: ETL_MONGO_DB, default "nasa_etl".
"""
from __future__ import annotations

import os

from pymongo import ASCENDING, MongoClient

from etl import config
from etl.pipeline import Q2_LIMIT, BatchResult, Pipeline, QueryResults

# SPEC.md section 2.1, unchanged. Group numbers in the comments are the
# spec's; $regexFind's "captures" array is 0-based, so group n is index n-1.
LINE_REGEX = (
    r'^(\S+) \S+ \S+ \[((0[1-9]|[12]\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/'
    r'(\d{4}):([01]\d|2[0-3]):[0-5]\d:[0-5]\d [+-]\d{4})\] "(.*)" (\d{3}) (\d+|-)$'
)
# SPEC.md section 2.3, unchanged.
REQUEST_REGEX = r'^(\S+)\s+(.+?)(?:\s+(HTTP/\S+))?$'

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUMBERS = [f"{i:02d}" for i in range(1, 13)]

INSERT_CHUNK = 20_000  # documents per insert_many call


def _db_name() -> str:
    return os.environ.get("ETL_MONGO_DB", "nasa_etl")


def _raw_name(run_id: str) -> str:
    return f"raw_lines_{run_id}"


def _parsed_name(run_id: str) -> str:
    return f"parsed_logs_{run_id}"


def _capture(match_var: str, index: int) -> dict:
    """Capture `index` (0-based) of a $regexFind result, "" if the group did not take part."""
    return {"$ifNull": [{"$arrayElemAt": [f"$${match_var}.captures", index]}, ""]}


def _strip_one_cr(field: str) -> dict:
    """The line without one trailing "\\r" (SPEC.md section 1)."""
    length = {"$strLenCP": field}
    return {"$cond": [
        {"$and": [
            {"$gt": [length, 0]},
            # $cond only evaluates the chosen branch, so $substrCP never sees -1.
            {"$eq": [{"$cond": [{"$gt": [length, 0]},
                                {"$substrCP": [field, {"$subtract": [length, 1]}, 1]},
                                ""]}, "\r"]},
        ]},
        {"$substrCP": [field, 0, {"$subtract": [length, 1]}]},
        field,
    ]}


def parse_pipeline(batch_id: int, into: str) -> list[dict]:
    """Aggregation over raw_lines that parses one batch into `into` (SPEC.md section 2)."""
    return [
        {"$match": {"batch_id": batch_id}},
        {"$project": {
            "_id": 1, "batch_id": 1, "line_no": 1,
            "m": {"$regexFind": {"input": _strip_one_cr("$line"), "regex": LINE_REGEX}},
        }},
        {"$project": {
            "_id": 1, "batch_id": 1, "line_no": 1,
            "malformed": {"$eq": [{"$ifNull": ["$m", None]}, None]},
            "f": {"$let": {
                "vars": {"m": "$m"},
                "in": {
                    "host": _capture("m", 0),
                    "timestamp": _capture("m", 1),
                    "day": _capture("m", 2),
                    "month": _capture("m", 3),
                    "year": _capture("m", 4),
                    "hour": _capture("m", 5),
                    # Trim spaces only (not tabs or other whitespace) from both ends.
                    "request": {"$trim": {"input": _capture("m", 6), "chars": " "}},
                    "status": _capture("m", 7),
                    "bytes": _capture("m", 8),
                },
            }},
        }},
        {"$set": {"r": {"$cond": [
            "$malformed", None,
            {"$regexFind": {"input": "$f.request", "regex": REQUEST_REGEX}},
        ]}}},
        {"$project": {
            "_id": 1, "batch_id": 1, "line_no": 1, "malformed": 1,
            "parsed": {"$cond": ["$malformed", "$$REMOVE", {"$let": {
                "vars": {"r": "$r"},
                "in": {
                    "host": "$f.host",
                    "timestamp": "$f.timestamp",
                    "log_date": {"$concat": [
                        "$f.year", "-",
                        {"$arrayElemAt": [MONTH_NUMBERS, {"$indexOfArray": [MONTH_NAMES, "$f.month"]}]},
                        "-", "$f.day",
                    ]},
                    "log_hour": {"$toInt": "$f.hour"},
                    # Request pattern matched: its groups (protocol "" if absent).
                    # Not matched: the whole trimmed request is the method.
                    "http_method": {"$cond": [{"$eq": ["$$r", None]}, "$f.request", _capture("r", 0)]},
                    "resource_path": {"$cond": [{"$eq": ["$$r", None]}, "", _capture("r", 1)]},
                    "protocol_version": {"$cond": [{"$eq": ["$$r", None]}, "", _capture("r", 2)]},
                    "status_code": {"$toInt": "$f.status"},
                    "bytes_transferred": {"$cond": [
                        {"$eq": ["$f.bytes", "-"]}, {"$toLong": 0}, {"$toLong": "$f.bytes"}]},
                },
            }}]},
        }},
        {"$replaceWith": {"$mergeObjects": [
            {"_id": "$_id", "batch_id": "$batch_id", "line_no": "$line_no", "malformed": "$malformed"},
            {"$ifNull": ["$parsed", {}]},
        ]}},
        {"$merge": {"into": into, "on": "_id", "whenMatched": "fail", "whenNotMatched": "insert"}},
    ]


def _scope_match(batch_id: int | None) -> dict:
    match: dict = {"malformed": False}
    if batch_id is not None:
        match["batch_id"] = batch_id
    return {"$match": match}


def q1_pipeline(batch_id: int | None) -> list[dict]:
    """Q1: daily traffic per (log_date, status_code)."""
    return [
        _scope_match(batch_id),
        {"$group": {
            "_id": {"log_date": "$log_date", "status_code": "$status_code"},
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes_transferred"},
        }},
        {"$sort": {"_id.log_date": 1, "_id.status_code": 1}},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "status_code": "$_id.status_code",
            "request_count": {"$toLong": "$request_count"},
            "total_bytes": {"$toLong": "$total_bytes"},
        }},
    ]


def q2_pipeline(batch_id: int | None) -> list[dict]:
    """Q2: top 20 resource paths by request count, ties by path in character-code order.

    Grouped first by (path, host) so distinct hosts are counted without
    building large host arrays.
    """
    return [
        _scope_match(batch_id),
        {"$group": {
            "_id": {"path": "$resource_path", "host": "$host"},
            "n": {"$sum": 1},
            "bytes": {"$sum": "$bytes_transferred"},
        }},
        {"$group": {
            "_id": "$_id.path",
            "request_count": {"$sum": "$n"},
            "total_bytes": {"$sum": "$bytes"},
            "distinct_host_count": {"$sum": 1},
        }},
        # No collation: MongoDB compares strings by their UTF-8 bytes, which is
        # the same order as character codes.
        {"$sort": {"request_count": -1, "_id": 1}},
        {"$limit": Q2_LIMIT},
        {"$project": {
            "_id": 0,
            "resource_path": "$_id",
            "request_count": {"$toLong": "$request_count"},
            "total_bytes": {"$toLong": "$total_bytes"},
            "distinct_host_count": {"$toLong": "$distinct_host_count"},
        }},
    ]


def q3_pipeline(batch_id: int | None) -> list[dict]:
    """Q3: hourly error analysis per (log_date, log_hour).

    Grouped first by (date, hour, host); a host counts as an error host for
    the hour if it has at least one record with 400 <= status <= 599.
    """
    is_error = {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]}
    return [
        _scope_match(batch_id),
        {"$group": {
            "_id": {"log_date": "$log_date", "log_hour": "$log_hour", "host": "$host"},
            "total": {"$sum": 1},
            "errors": {"$sum": {"$cond": [is_error, 1, 0]}},
        }},
        {"$group": {
            "_id": {"log_date": "$_id.log_date", "log_hour": "$_id.log_hour"},
            "total_request_count": {"$sum": "$total"},
            "error_request_count": {"$sum": "$errors"},
            "distinct_error_hosts": {"$sum": {"$cond": [{"$gt": ["$errors", 0]}, 1, 0]}},
        }},
        {"$sort": {"_id.log_date": 1, "_id.log_hour": 1}},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "log_hour": "$_id.log_hour",
            "error_request_count": {"$toLong": "$error_request_count"},
            "total_request_count": {"$toLong": "$total_request_count"},
            # 64-bit float division, not rounded.
            "error_rate": {"$divide": [{"$toDouble": "$error_request_count"},
                                       {"$toDouble": "$total_request_count"}]},
            "distinct_error_hosts": {"$toLong": "$distinct_error_hosts"},
        }},
    ]


def count_pipeline(batch_id: int) -> list[dict]:
    """Record, parsed and malformed counts of one batch, counted in MongoDB."""
    return [
        {"$match": {"batch_id": batch_id}},
        {"$group": {
            "_id": None,
            "records": {"$sum": 1},
            "malformed": {"$sum": {"$cond": ["$malformed", 1, 0]}},
            "parsed": {"$sum": {"$cond": ["$malformed", 0, 1]}},
        }},
    ]


def _iter_lines(batch_path: str):
    """Yields (line_no, text) for each record of a batch file.

    Splits only on "\\n" (a final "\\n" does not start an extra record; a last
    line without "\\n" is still a record) and decodes each line as Latin-1.
    Nothing is trimmed, cleaned or filtered: a trailing "\\r", empty lines and
    malformed lines all go to MongoDB as they are.
    """
    with open(batch_path, "rb") as f:
        for line_no, raw in enumerate(f, start=1):
            if raw.endswith(b"\n"):
                raw = raw[:-1]
            yield line_no, raw.decode("latin-1")


class MongoDBPipeline(Pipeline):
    name = "mongodb"

    def __init__(self):
        self._client: MongoClient | None = None

    @property
    def db(self):
        if self._client is None:
            self._client = MongoClient(config.mongo_uri())
        return self._client[_db_name()]

    def setup(self, run_id: str) -> None:
        for name in (_raw_name(run_id), _parsed_name(run_id)):
            self.db.drop_collection(name)
            self.db.create_collection(name)
        self.db[_raw_name(run_id)].create_index([("batch_id", ASCENDING)])
        self.db[_parsed_name(run_id)].create_index(
            [("batch_id", ASCENDING), ("malformed", ASCENDING)])

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        raw = self.db[_raw_name(run_id)]
        parsed = self.db[_parsed_name(run_id)]

        chunk = []
        for line_no, text in _iter_lines(batch_path):
            chunk.append({"batch_id": batch_id, "line_no": line_no, "line": text})
            if len(chunk) >= INSERT_CHUNK:
                raw.insert_many(chunk, ordered=False)
                chunk = []
        if chunk:
            raw.insert_many(chunk, ordered=False)

        # Parse inside MongoDB, then count inside MongoDB.
        raw.aggregate(parse_pipeline(batch_id, _parsed_name(run_id)), allowDiskUse=True)
        record_count = raw.count_documents({"batch_id": batch_id})
        counts = next(parsed.aggregate(count_pipeline(batch_id)),
                      {"records": 0, "parsed": 0, "malformed": 0})
        if counts["records"] != record_count:
            raise RuntimeError(
                f"batch {batch_id}: {record_count} raw lines loaded but "
                f"{counts['records']} parsed documents written")
        return BatchResult(record_count, int(counts["parsed"]), int(counts["malformed"]))

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        coll = self.db[_parsed_name(run_id)]

        def run(pipeline):
            return list(coll.aggregate(pipeline, allowDiskUse=True))

        q1 = [(d["log_date"], int(d["status_code"]), int(d["request_count"]), int(d["total_bytes"]))
              for d in run(q1_pipeline(batch_id))]
        q2 = [(d["resource_path"], int(d["request_count"]), int(d["total_bytes"]),
               int(d["distinct_host_count"]))
              for d in run(q2_pipeline(batch_id))]
        q3 = [(d["log_date"], int(d["log_hour"]), int(d["error_request_count"]),
               int(d["total_request_count"]), float(d["error_rate"]), int(d["distinct_error_hosts"]))
              for d in run(q3_pipeline(batch_id))]
        return QueryResults(q1=q1, q2=q2, q3=q3)

    def cleanup(self, run_id: str) -> None:
        if os.environ.get("ETL_MONGO_KEEP"):  # keep collections for debugging
            return
        for name in (_raw_name(run_id), _parsed_name(run_id)):
            self.db.drop_collection(name)
        if self._client is not None:
            self._client.close()
            self._client = None
