# MongoDB pipeline (owner: Navya)

## How it works

| Step | Where it runs | What happens |
|---|---|---|
| `setup` | MongoDB | Creates empty `raw_lines_<run_id>` and `parsed_logs_<run_id>` in database `nasa_etl` (override with `ETL_MONGO_DB`), with an index on `batch_id` |
| `process_batch` load | Python → MongoDB | Splits the batch file on `\n` only and inserts every line unchanged (Latin-1 decoded) with `batch_id` and `line_no`. No trimming, filtering or parsing. |
| `process_batch` parse | MongoDB aggregation | Removes one trailing `\r`, applies the SPEC.md 2.1 regex with `$regexFind`, flags non-matches as `malformed`, trims the request with `$trim` (spaces only), applies the 2.3 request regex, derives `log_date`, `log_hour`, `status_code`, `bytes_transferred` (`-` → 0, 64-bit), and writes every line to `parsed_logs` with `$merge` |
| `process_batch` counts | MongoDB aggregation | Records, parsed and malformed are counted with `$group` |
| `run_queries` | MongoDB aggregation | Q1, Q2, Q3 with `$group`, `$sort`, `$limit`. Per batch: `$match` on `batch_id`. Whole run: every parsed record, never a sum of batch results. Distinct hosts use a two-level group (by key + host, then by key). `error_rate` is a `$divide` of two doubles. |
| `cleanup` | MongoDB | Drops both collections. Set `ETL_MONGO_KEEP=1` to keep them for inspection. |

Python only frames lines, starts aggregations and converts BSON numbers to Python `int`/`float` when reading the results back. It does no sorting or arithmetic.

## Run it

```bash
brew services start mongodb-community@8.0
scripts/dev_postgres.sh start

.venv/bin/pytest pipelines/mongodb -v     # MongoDB vs reference, no PostgreSQL needed

# golden sample (once Saravanan adds it)
.venv/bin/python tests/run_reference.py --input tests/golden/sample.log --batch-size 50
.venv/bin/etl run --pipeline mongodb --input tests/golden/sample.log --batch-size 50
.venv/bin/etl runs
.venv/bin/etl compare --runs <reference_run_id> <mongodb_run_id>

# full data
scripts/download_data.sh
.venv/bin/python tests/run_reference.py --batch-size 500000
.venv/bin/etl run --pipeline mongodb --batch-size 500000
.venv/bin/etl compare --runs <reference_run_id> <mongodb_run_id>
.venv/bin/etl verify --run <mongodb_run_id>
.venv/bin/etl report --latest
```

The full run should show 3,461,613 records, 3,461,612 parsed, 1 malformed, 8 batches, and Q2 top path `/images/NASA-logosmall.gif` with 208,723 requests, 131,725,740 bytes and 81,510 distinct hosts.
