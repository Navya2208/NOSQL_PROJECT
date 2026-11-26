# SPEC: shared rules for every pipeline

Every pipeline (MongoDB, MapReduce, Pig, Hive) implements exactly these rules. If a rule here is unclear, or a pipeline needs different behaviour, change this file first and then change every pipeline. Never fix something inside one pipeline only.

Owner: Aprajita. Status: **agreed by all four team members on 18 November 2026**. Checked against the full official log files the same day (section 12). Added on 23 November 2026: the Java regex note (section 2.1) and the rule that parsing is never written in Python files (section 4).

---

## 1. Input

| Rule | Value |
|---|---|
| Input files | `NASA_access_log_Jul95` and `NASA_access_log_Aug95`, decompressed from the official `.gz` files and otherwise unmodified |
| Location | `data/` in the repository root. Not committed to Git. |
| Read order | July file first, then August file. The two files are never joined (section 3). |
| One record | One line. Lines are split on `\n`. One trailing `\r` is removed if present. A `\n` at the very end of a file does not create an extra empty record. A last line without `\n` is still a record: the July file ends with a cut-off line (`alyssa.p`), which is a malformed record. |
| Empty lines | An empty line in the middle of a file is a record, and it is malformed. |
| Character decoding | Bytes are decoded as ISO-8859-1 (Latin-1), which maps every byte to one character and never fails. Checked: 18 lines contain non-ASCII bytes (8 in July, 10 in August), all rare 404 paths or binary garbage requests answered with status 400. An engine that reads text as UTF-8 may show those bytes differently, which can only change those rare `http_method` and `resource_path` strings. It cannot change Q1, Q3 or the whole-run top 20 of Q2. |

No cleaning, filtering, editing or format conversion happens outside the selected pipeline.

---

## 2. Parsing

### 2.1 Line pattern

A record is **parsed** if the whole line matches this regular expression. Otherwise it is **malformed**.

```
^(\S+) \S+ \S+ \[((0[1-9]|[12]\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\d{4}):([01]\d|2[0-3]):[0-5]\d:[0-5]\d [+-]\d{4})\] "(.*)" (\d{3}) (\d+|-)$
```

| Group | Field |
|---|---|
| 1 | `host` |
| 2 | `timestamp`, the full text between `[` and `]` |
| 3 | day |
| 4 | month name |
| 5 | year |
| 6 | hour |
| 7 | request, the text between the outer quotes |
| 8 | `status_code` |
| 9 | bytes |

Implementation notes, so every engine behaves the same:

- MongoDB (`$regexFind`), Java (MapReduce, Pig, Hive) and Python all use backtracking regex engines with the same meaning for this pattern.
- Python code compiles patterns with `re.ASCII`, because without it `\d`, `\s` and `\S` also match non-ASCII characters and Java does not.
- The match must cover the whole line. Use `re.fullmatch` or the anchors above, `matches()` in Java, and keep `^`…`$` in MongoDB.
- **Java engines (Pig, Hive, Java MapReduce):** put `(?d)` in front of both regexes. Without it, Java's `.` does not match U+0085 (byte `0x85` decoded as Latin-1) and `$` treats some characters as line endings, so a few real lines would parse differently. `(?d)` (UNIX_LINES) makes `\n` the only line terminator, which is how Python and MongoDB already behave. Also decode input as Latin-1: Java and Hadoop text readers default to UTF-8.

### 2.2 Derived fields

| Field | Rule | Type |
|---|---|---|
| `host` | Group 1 as written. Case-sensitive. | string |
| `timestamp` | Group 2 as written, for example `01/Jul/1995:00:00:01 -0400` | string |
| `log_date` | `YYYY-MM-DD` from groups 5, 4 and 3, with the month name mapped Jan=01 … Dec=12. No timezone conversion. | string, stored as DATE |
| `log_hour` | Group 6 as an integer, 0 to 23. No timezone conversion. | integer |
| `status_code` | Group 8 as an integer | integer |
| `bytes_transferred` | Group 9 as an integer. `-` becomes 0. | 64-bit integer |

### 2.3 Request field

Trim spaces from both ends of group 7, then match it against:

```
^(\S+)\s+(.+?)(?:\s+(HTTP/\S+))?$
```

| Case | `http_method` | `resource_path` | `protocol_version` |
|---|---|---|---|
| Pattern matches | group 1 | group 2 | group 3, or `""` if group 3 did not match |
| Pattern does not match (zero or one token) | the whole trimmed request, possibly `""` | `""` | `""` |

Missing values are always the empty string `""`, never NULL. MongoDB returns `null` and Java returns `null` for a group that did not take part in the match, so convert those to `""`.

A record with an unusual request is still a parsed record. Only the line pattern in 2.1 decides whether a record is malformed.

### 2.4 Examples

These show the log format with made-up hosts. Replace them with real lines from the golden sample once it exists.

| Line | Result |
|---|---|
| `host-a.example.com - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245` | parsed. date `1995-07-01`, hour 0, `GET`, `/history/apollo/`, `HTTP/1.0`, 200, 6245 |
| `host-b.example.com - - [01/Jul/1995:13:05:12 -0400] "GET /images/logo.gif HTTP/1.0" 304 -` | parsed. hour 13, bytes 0 |
| `host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html" 200 1024` | parsed. protocol `""` |
| `host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /some file.html HTTP/1.0" 200 512` | parsed. path `/some file.html` |
| `host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "" 400 0` | parsed. method, path and protocol all `""` |
| `host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html HTTP/1.0" 200` | malformed: no bytes field |
| `host-e.example.com - - [02/Jul/1995 08:15:00 -0400] "GET / HTTP/1.0" 200 100` | malformed: timestamp has a space instead of `:` |
| *(empty line)* | malformed |

Checked on the full data: exactly 1 of 3,461,613 records is malformed, the cut-off `alyssa.p` line at the end of the July file. The pattern is not too strict.

---

## 3. Batching

| Rule | Value |
|---|---|
| Batch size `N` | Number of input records per batch, chosen with `--batch-size`. The same `N` is used for every run in one comparison. |
| Files are never joined | Each input file is cut into batches on its own, so a batch never contains lines from two files. Joining would hide the cut-off last line of the July file. |
| Batch IDs | Start at 1 and continue across files. Example with July split into 4 batches: August starts at batch 5. |
| Last batch of each file | May hold fewer than `N` records and still counts as a batch. |
| Unchanged bytes | The controller splits only on `\n` and copies the bytes exactly: no decoding, trimming, cleaning or filtering, and no newline added to a last line that has none. Pipelines remove a trailing `\r` themselves (section 1). |
| What is counted | Every record, malformed ones included. |
| Average batch size | `total_records / number of non-empty batches` |
| Count check | For each batch, the pipeline counts the records it reads. Its count and `parsed + malformed` must equal the number of lines the controller sent, or the run stops. |
| Proof of untouched slices | For each batch the controller records the source file, first and last line number, and the SHA-256 of the batch bytes. `etl verify --run RUN_ID` re-reads the raw files and checks every hash. |

The controller writes each batch to `work/<run_id>/batch_<k>.log` and gives that path to the pipeline. Cutting the input into unchanged slices is batching, not preprocessing. All parsing, malformed-line detection and aggregation happen inside the pipeline's engine. Converting to CSV or JSON, parsing in Python, or removing bad lines before the engine sees them would count as preprocessing and is not allowed.

---

## 4. Pipeline interface

Every pipeline is a Python class in `pipelines/<name>/` that implements this interface. The controller calls only these methods, so a pipeline can be built and tested without the real controller.

```python
from dataclasses import dataclass

@dataclass
class BatchResult:
    record_count: int      # lines received in this batch
    parsed_count: int
    malformed_count: int   # parsed_count + malformed_count == record_count

@dataclass
class QueryResults:
    q1: list[tuple]  # (log_date: str, status_code: int, request_count: int, total_bytes: int)
    q2: list[tuple]  # (resource_path: str, request_count: int, total_bytes: int, distinct_host_count: int)
    q3: list[tuple]  # (log_date: str, log_hour: int, error_request_count: int,
                     #  total_request_count: int, error_rate: float, distinct_error_hosts: int)

class Pipeline:
    name: str  # "mongodb", "mapreduce", "pig" or "hive"

    def setup(self, run_id: str) -> None:
        """Create empty working storage for this run (collections, HDFS dirs, tables)."""

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        """Load and parse one batch with this pipeline's technology."""

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        """Run Q1, Q2 and Q3 over batch k's parsed records (batch_id = k),
        or over every parsed record of the run (batch_id = None)."""

    def cleanup(self, run_id: str) -> None:
        """Remove working storage. Never touches PostgreSQL."""
```

Rules:

- All parsing and all query logic run inside the pipeline's technology: a MongoDB aggregation, a Hadoop job, a Pig script or a Hive query. Python in the pipeline class only starts jobs and reads their output back.
- **Parsing is never written in Python files.** It is written in the engine's own language so the engine can split and parallelise it: a MongoDB aggregation, a Java MapReduce mapper, Pig Latin, or HiveQL with `RegexSerDe`. A Java function that only decodes bytes into text, without parsing, is allowed.
- The controller calls `process_batch(k)` then `run_queries(run_id, k)` for each batch, and `run_queries(run_id)` once after the last batch. The engine must therefore keep every batch's parsed records until the run ends.
- `run_queries` returns rows in exactly the column order above. Reading Hadoop or Hive output files into tuples is allowed. Recomputing or re-sorting values in Python is not.
- `setup` and `cleanup` must leave nothing from earlier runs that could change counts.

---

## 5. Queries

All three queries use **parsed records only**. Malformed records are counted in `runs` and `batches` but never enter a query.

Every query runs in two scopes:

| Scope | Stored with | Computed over |
|---|---|---|
| Per batch | `batch_id` = 1 … n | Only that batch's parsed records. These are real answers for that batch, for example Q2 is the top 20 within the batch. |
| Whole run | `batch_id` = 0 | Every parsed record of the run, computed once after the last batch. This is the final answer the report shows. |

The whole-run answer is never built by adding up per-batch results: distinct host counts and top-20 lists cannot be added.

### Q1: Daily traffic summary

- Group by (`log_date`, `status_code`).
- `request_count` = number of records. `total_bytes` = sum of `bytes_transferred`.
- Return every group.
- Columns: `log_date, status_code, request_count, total_bytes`

### Q2: Top requested resources

- Group by `resource_path`, including `""` as its own value.
- `request_count` = number of records. `total_bytes` = sum of `bytes_transferred`. `distinct_host_count` = number of different `host` values.
- Sort by `request_count` descending, then `resource_path` ascending by character code. Return the first 20.
- Columns: `resource_path, request_count, total_bytes, distinct_host_count`

### Q3: Hourly error analysis

- Group by (`log_date`, `log_hour`). Include every hour that has at least one parsed record, even with zero errors. Hours with no records at all do not appear.
- An error record has `400 <= status_code <= 599`.
- `error_request_count` = number of error records. `total_request_count` = number of records.
- `error_rate` = `error_request_count / total_request_count`, computed as a 64-bit float and **not rounded**. A division of the same two integers gives the identical float in every language, so pipelines can be compared exactly. The report shows it to 4 decimal places.
- `distinct_error_hosts` = number of different `host` values among error records, 0 if there are none.
- Columns: `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`

Sums and counts use 64-bit integers in every pipeline, for example `BIGINT` in Hive and `long` in Pig. Daily byte totals can exceed the 32-bit limit.

---

## 6. PostgreSQL schema

The tables are defined in `sql/schema.sql`, which is the single source of truth. Create them with `etl init-db`.

| Table | Holds |
|---|---|
| `runs` | One row per run: pipeline, batch size, input files, status, start and finish time, runtime, record totals, batch count, average batch size, error message if it failed |
| `batches` | One row per batch: record, parsed and malformed counts, source file, first and last line number, SHA-256 |
| `q1_daily_traffic` | Q1 rows plus `run_id`, `pipeline`, `batch_id`, `executed_at` |
| `q2_top_resources` | Q2 rows plus the same four metadata columns |
| `q3_hourly_errors` | Q3 rows plus the same four metadata columns |

The pipeline name is not restricted in the database; the CLI only accepts `mongodb`, `mapreduce`, `pig` and `hive`.

- `batch_id` on result rows: 1 … n for per-batch results, **0 for whole-run results** (section 5). 0 is used instead of NULL because `batch_id` is part of each result table's primary key.
- `executed_at` is the time that scope's queries finished: after each batch for per-batch rows, after the last batch for whole-run rows.
- `resource_path` uses `COLLATE "C"` so PostgreSQL sorts by character code, the same order as Q2's tie-break.
- All result rows of a run are written in one transaction.

---

## 7. Runtime

| Point | When |
|---|---|
| Before the clock | Creating the `runs` row with status `running`, and `pipeline.setup()` |
| Clock starts | Immediately before the first input file is opened |
| Inside the clock | Cutting batches, parsing, per-batch queries, whole-run queries, loading |
| Clock stops | Immediately after the transaction holding all result rows commits |
| After the clock | Updating the `runs` row with `finished_at`, `runtime_seconds` and the totals, `cleanup()`, and the report |

Measured with a monotonic clock (`time.perf_counter()` in Python). Dataset download, software installation and report rendering are never included.

---

## 8. Command line

```bash
etl                                           # menu: choose pipeline, batch size and input, then run
etl init-db [--reset]                         # create the tables; --reset drops them first
etl run --pipeline {mongodb,mapreduce,pig,hive} --batch-size N [--input FILE ...] [--keep-work]
etl runs                                      # list recent runs and their IDs
etl report --run RUN_ID [--batch K] [--rows N]  # or --latest; whole run by default, --batch K for one batch
etl compare --runs RUN_ID_A RUN_ID_B          # exit code 0 if identical, 1 if not
etl verify --run RUN_ID                       # prove every batch is an unchanged slice of the raw files
```

`etl run` uses both input files from `data/` by default. `--input` overrides this for testing with the golden sample.

---

## 9. Equivalence check

- **Golden sample:** `tests/golden/sample.log` holds 346 lines copied unchanged from the official files by `pipelines/mapreduce/make_golden_sample.py`, covering every case that occurs in the real data: the cut-off `alyssa.p` line, binary one-token requests, non-ASCII bytes, odd and missing protocols, spaces in paths, `-` bytes and every status code. It contains only real lines. The section 2.4 cases that never occur in the real data (empty request, empty line, bad timestamp) are tested with synthetic lines in `tests/conftest.py` and in each pipeline's own tests. Expected results come from `tests/run_reference.py`, which runs `tests/reference_pipeline.py` (a plain-Python version of this spec, used only for testing) through the real controller. Check those results by hand once. Owner: Saravanan.
- **Compare script:** `etl compare` checks two runs and prints every difference:
  - `total_records`, `parsed_records`, `malformed_records` and `batch_count` are equal;
  - every batch has the same source file, line range, counts and SHA-256;
  - all rows of Q1, Q2 and Q3 are equal in both scopes, per batch and whole run, including `error_rate`, compared exactly.
- **Same conditions:** compared runs use the same input files and the same batch size.
- **When:** every pipeline is compared with the reference run on the golden sample before its first full run, and with MongoDB's full run after that.

---

## 10. Repository layout

```
pyproject.toml        package and the `etl` command
SPEC.md
etl/                  controller, CLI, pipeline interface, loader, report, compare, verify
pipelines/mongodb/
pipelines/mapreduce/
pipelines/pig/
pipelines/hive/
sql/schema.sql
scripts/              dev_postgres.sh, dev_mongodb.sh, dev_pig.sh (local installs), mapreduce_env.sh, download_data.sh
tests/                tests, golden sample, reference_pipeline.py (test-only Python version of this spec)
data/                 raw log files, not committed
work/                 per-run batch files and the dev database, not committed
```

---

## 11. Questions and answers

| Question | Answer |
|---|---|
| What should `batch_id` on a result row mean? | Answered: per-batch rows use 1 … n, whole-run rows use 0 (sections 5 and 6). |
| Is the controller cutting batches "preprocessing"? | Answered: no, as long as the slices are unchanged bytes, the files are never joined, and the engine does all parsing (section 3). |
| Do the input files contain non-ASCII bytes? | Checked: 18 lines do. Decode as Latin-1; they cannot affect the whole-run results (section 1). |

---

## 12. Facts from the full data

Checked on 18 November 2026 with the reference pipeline (`tests/run_reference.py --batch-size 500000`). Every real pipeline must reproduce these numbers.

| Fact | Value |
|---|---|
| Lines in `NASA_access_log_Jul95` | 1,891,715, last line `alyssa.p` with no newline |
| Lines in `NASA_access_log_Aug95` | 1,569,898 |
| Total records | 3,461,613 |
| Parsed / malformed | 3,461,612 / 1 |
| Batches at batch size 500,000 | 8 (July 4, August 4); average batch size 432,701.62 |
| Requests with no protocol | 6,172 |
| Lines with bytes `-` | 33,904 |
| One-token requests (empty path) | 11, all binary garbage with status 400 |
| Protocol values | `HTTP/1.0`, `HTTP/V1.0`, `HTTP/*`, or empty |
| Status codes | 200, 302, 304, 400, 403, 404, 500, 501 |
| Q2 whole-run top path | `/images/NASA-logosmall.gif`: 208,723 requests, 131,725,740 bytes, 81,510 distinct hosts |
| Line endings | `\n` only; no `\r\n` and no empty lines |
| Reference run time | about 24 s (plain Python, for comparison only) |
