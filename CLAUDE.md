# CLAUDE.md: project guide for teammates and their AI assistants

Read this first. It explains every file in this folder, the rules that must not be broken, and what each team member does next. Claude Code loads this file automatically; with any other assistant, attach it to the conversation.

## The project

DAS 839 NoSQL Systems, end-semester project. One tool runs the same ETL and analytics workload on the NASA HTTP web server logs (July and August 1995) with a choice of four backends: **MongoDB, MapReduce, Pig or Hive**. Results and run metadata go into PostgreSQL, and a separate report shows them.

- **Phase 1** (review on 20 November 2026): done.
- **Phase 2:** all four pipelines, a compact report, a video and the Git repository link.

The shared tool is built and tested. All four pipelines (MongoDB, MapReduce, Pig, Hive) are built and give results identical to the reference on the golden sample and the full data (final checks 26 November 2026). Final timings come from Navya's benchmark machine (`benchmark/`).

## Team

| Person | Owns | Status |
|---|---|---|
| Aprajita | Shared tool (`etl/`), schema, setup scripts, `SPEC.md`; **Pig pipeline** (`pipelines/pig/`) | Done |
| Navya | MongoDB pipeline (`pipelines/mongodb/`); **benchmark machine**: merges everyone's folders into Git, installs all 4 engines, runs the final checks and timings; report comparison section | MongoDB done; merging in progress |
| Saravanan | **Hive pipeline** (`pipelines/hive/`); golden sample | In progress |
| Jatin | **Finish MapReduce** (`pipelines/mapreduce/`): parse and query jobs in Java | In progress |

Each person writes the report section for their own work.

## Rules that must not be broken

These come from the project statement and from `SPEC.md`, which all four members agreed on 18 November 2026.

1. **All parsing, malformed-line detection and aggregation must run inside the pipeline's own technology**: a MongoDB aggregation, a Hadoop job, a Pig script or a Hive query. Python in a pipeline class may only start that work and read results back. Parsing or computing results in Python counts as cheating the brief.
2. **No preprocessing outside the pipeline.** Never clean, filter, convert to CSV or JSON, or edit the raw files.
3. **Follow `SPEC.md` exactly**: the same regex, field rules, query definitions and tie-breaks in every pipeline. If a rule seems wrong, ask Aprajita to change `SPEC.md` first. Never fix it inside one pipeline only.
4. **Only edit your own `pipelines/<name>/` folder.** Do not change `etl/`, `sql/`, `tests/` (except adding the golden sample) or `SPEC.md` without asking Aprajita.
5. **Two result scopes.** `run_queries(run_id, batch_id=k)` covers only batch k's records; `run_queries(run_id)` covers every parsed record of the run. Never build the whole-run answer by adding up per-batch results.
6. **Exact output shapes.** Return the tuples defined in `etl/pipeline.py`. `log_date` is a `"YYYY-MM-DD"` string, `error_rate` is the unrounded float `errors / total`, and a missing method, path or protocol is `""`, never `None`.
7. **No Python files for parsing.** Parsing must be written in the engine's own language so the engine can split it and run it in parallel: a MongoDB aggregation, a **Java** MapReduce mapper, Pig Latin, or HiveQL with `RegexSerDe`. A small Java function that only turns bytes into text (like Pig's `Latin1TextLoader`) is fine; it does no parsing. Java engines must put `(?d)` in front of both regexes and decode input as Latin-1 (`SPEC.md` section 2.1).

## Every file, and who edits it

### Documents

| File | What it is | Edit? |
|---|---|---|
| `CLAUDE.md` | This guide | Aprajita |
| `SPEC.md` | **The contract every pipeline follows**: input, parsing regex, batching, pipeline interface, query definitions, schema, runtime, commands, equivalence checks, and facts from the full data (section 12) | Aprajita only |
| `README.md` | Setup, commands, how to add a pipeline | Aprajita |
| `NoSQL26_ET_project_statement.pdf` | The official project statement from the instructor | Never |

### Shared tool: `etl/` (built and tested; do not edit)

| File | What it does |
|---|---|
| `etl/cli.py` | The `etl` command: `init-db`, `run`, `runs`, `report`, `compare`, `verify`; plain `etl` opens the menu |
| `etl/menu.py` | The menu: choose one of the four pipelines, a batch size and the input, run it, show the report |
| `etl/controller.py` | Runs one ETL flow: cuts batches, calls the pipeline, checks counts, validates results, loads them, times the run |
| `etl/batching.py` | Cuts each raw file into batches of unchanged lines (never joining the two files) and records each batch's line range and SHA-256 |
| `etl/pipeline.py` | **The `Pipeline` base class every pipeline implements**, the result shapes, and the validation that rejects rows breaking `SPEC.md` |
| `etl/loader.py` | Writes runs, batches and per-batch and whole-run results to PostgreSQL in one transaction |
| `etl/report.py` | `etl report` and `etl runs`: results plus execution metadata, read only from PostgreSQL |
| `etl/compare.py` | `etl compare`: checks two runs give identical totals, batch slices and Q1–Q3 rows |
| `etl/verify.py` | `etl verify`: re-reads the raw files and proves every batch was an unchanged slice |
| `etl/db.py` | PostgreSQL connection and table creation |
| `etl/config.py` | Settings from environment variables: `ETL_PG_DSN`, `ETL_MONGO_URI`, `ETL_DATA_DIR`, `ETL_WORK_DIR` |
| `etl/textfmt.py` | Plain-text tables for the terminal |
| `etl/__init__.py`, `etl/__main__.py` | Package files; `python -m etl` also works |

### Pipelines: `pipelines/` (the work to do)

| File | Owner | Status |
|---|---|---|
| `pipelines/mongodb/pipeline.py` | Navya | **Built.** Identical to the reference on the full data. A worked example for the other pipelines. |
| `pipelines/mapreduce/pipeline.py` | Jatin (with Saravanan) | **Built** with Hadoop Streaming and identical to the reference on the full data. Being rewritten in Java to follow rule 7. Its README explains every file and the settings. |
| `pipelines/pig/pipeline.py` | Aprajita | **Built** (Pig Latin: `parse.pig`, `queries.pig`, plus a small Latin-1 Java loader). Identical to the reference on the golden sample and the full data; works with or without a local Hadoop. Its README explains every file. Test: `.venv/bin/pytest pipelines/pig` (about 2 minutes). |
| `pipelines/hive/pipeline.py` | Saravanan | In progress |

`pipelines/mongodb/` also holds Navya's handover notes (`CLAUDE.md`, `README.md`) and her test `test_mongodb_pipeline.py`, which needs MongoDB but not PostgreSQL: `.venv/bin/pytest pipelines/mongodb`.

Each unbuilt starter raises `NotImplementedError` in `__init__` so it cannot run until built. Remove that line when you implement it. Extra files such as mapper and reducer scripts, `.pig` files or `.sql` files go in the same folder.

### Database and setup

| File | What it is |
|---|---|
| `sql/schema.sql` | The five PostgreSQL tables: `runs`, `batches`, `q1_daily_traffic`, `q2_top_resources`, `q3_hourly_errors` |
| `scripts/dev_postgres.sh` | Private local PostgreSQL on `localhost:5433`, no password |
| `scripts/dev_mongodb.sh` | Private local MongoDB on `localhost:27017`, downloaded from MongoDB's official build on first start (macOS) |
| `scripts/dev_pig.sh` | Installs Apache Pig 0.18.0 (Hadoop 3 build, about 492 MB) into `work/pig` after checking Apache's checksum: `scripts/dev_pig.sh install` |
| `scripts/mapreduce_env.sh` | Sets `HADOOP_HOME`, `ETL_HDFS_ROOT` and `ETL_MR_PYTHON` for the MapReduce pipeline: `source scripts/mapreduce_env.sh` |
| `scripts/download_data.sh` | Downloads and decompresses the two official NASA log files into `data/` |
| `pyproject.toml` | Python package definition and the `etl` command |
| `.gitignore` | Keeps data, `work/`, `.venv/` and caches out of Git |
| `data/.gitkeep`, `tests/golden/.gitkeep` | Keep empty folders in Git |

### Tests

| File | What it is |
|---|---|
| `tests/reference_pipeline.py` | Plain-Python version of `SPEC.md`, **for tests only**. Gives trusted results to compare against. Never use it as one of the four pipelines. |
| `tests/run_reference.py` | Runs the reference pipeline through the real controller, so `etl compare` can check a pipeline against it |
| `tests/test_spec_parsing.py` | The `SPEC.md` parsing examples |
| `tests/test_batching.py` | Batching rules: unchanged bytes, files never joined, hashes |
| `tests/test_end_to_end.py` | Full flow against PostgreSQL: run, report, compare, verify, error cases |
| `tests/test_menu.py` | The `etl` menu, with scripted answers |
| `tests/conftest.py` | Shared test data and a clean `etl_test` database |
| `tests/golden/sample.log` | Golden sample: 346 real lines copied unchanged from the official files by `pipelines/mapreduce/make_golden_sample.py`. Real lines only; synthetic edge cases live in `tests/conftest.py`. |

## How a run works

```
etl run --pipeline mongodb --batch-size 500000
  controller: cut July, then August, into batch files (unchanged bytes, files never joined)
  for each batch k:
      pipeline.process_batch(run_id, k, batch_path)  -> parse inside the engine, count malformed
      pipeline.run_queries(run_id, k)                -> Q1-Q3 over batch k only    (stored with batch_id = k)
  pipeline.run_queries(run_id)                       -> Q1-Q3 over all records     (stored with batch_id = 0)
  loader: one PostgreSQL transaction; runtime clock stops at commit
etl report --latest                                  -> whole-run results; --batch K for one batch
```

The controller stops the run if a pipeline's record count for a batch differs from the lines it was sent, or if any result row breaks `SPEC.md`. The error names the batch, query, row and rule.

## Setup

Everything runs as local installs; Docker is not used.

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
scripts/dev_postgres.sh start        # needs PostgreSQL installed: brew install postgresql@16
scripts/dev_mongodb.sh start         # MongoDB pipeline only; downloads MongoDB on first start
source scripts/mapreduce_env.sh      # MapReduce and Pig; set HADOOP_HOME first if Hadoop is elsewhere
scripts/dev_pig.sh install           # Pig pipeline only; downloads Pig 0.18.0 once
.venv/bin/etl init-db
.venv/bin/pytest                     # all tests should pass
scripts/download_data.sh             # about 37 MB download
```

MongoDB (MongoDB pipeline only): `scripts/dev_mongodb.sh start`. The first start downloads MongoDB's official build (8.0.38, about 82 MB, macOS) into `work/mongodb`; it then runs on `localhost:27017`. No Homebrew or Docker needed. On Linux or Windows, install MongoDB 8.0 from mongodb.com instead.

## Implementing a pipeline

1. Open `pipelines/<name>/pipeline.py`, read its to-do list, and remove the `NotImplementedError` in `__init__`.
2. Implement the four methods from `etl/pipeline.py`:
   - `setup(run_id)`: create empty working storage for the run.
   - `process_batch(run_id, batch_id, batch_path) -> BatchResult(record_count, parsed_count, malformed_count)`: load the batch file into the engine and parse it there. Remove a trailing `\r` if present. The last line of a batch may have no newline.
   - `run_queries(run_id, batch_id=None) -> QueryResults(q1, q2, q3)`: run the three queries in the engine, over one batch or the whole run. Keep every batch's parsed records until `cleanup`.
   - `cleanup(run_id)`: remove working storage. Never touch PostgreSQL.
3. Decode raw bytes as Latin-1 wherever text is needed (`SPEC.md` section 1). 18 lines contain non-ASCII bytes.

## Checking your work

1. `.venv/bin/pytest` must still pass.
2. Compare with the reference on the golden sample, then on the full data:

   ```bash
   .venv/bin/python tests/run_reference.py --input tests/golden/sample.log --batch-size 50
   .venv/bin/etl run --pipeline <name> --input tests/golden/sample.log --batch-size 50
   .venv/bin/etl runs
   .venv/bin/etl compare --runs <reference_run_id> <your_run_id>
   ```

3. A full run at batch size 500,000 must reproduce `SPEC.md` section 12, including:
   - 3,461,613 records, 3,461,612 parsed, 1 malformed (July's cut-off last line `alyssa.p`);
   - 8 batches, 4 per file, average batch size 432,701.62;
   - whole-run Q2 top path `/images/NASA-logosmall.gif`: 208,723 requests, 131,725,740 bytes, 81,510 distinct hosts.
4. `etl verify --run <run_id>` must report every batch as an unchanged slice.

## Not included

- **NASA log files:** download with `scripts/download_data.sh` (about 370 MB unzipped). Never commit or modify them.
- **`.venv/`, `work/`, the local database:** created on your machine by the setup steps.
- **Git history:** Navya creates the GitHub repository and merges everyone's work into it.

## When you are done

Send Navya your `pipelines/<name>/` folder with any job scripts in it, plus your report section. Navya merges everything into the GitHub repository and runs the final checks and timings on her benchmark machine.
