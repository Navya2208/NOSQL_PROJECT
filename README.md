# NASA Log Multi-Pipeline ETL

One tool that runs the same ETL and analytics workload on the NASA HTTP web server logs with a choice of four backends: **MongoDB**, **MapReduce**, **Pig** or **Hive**. Results and run metadata are loaded into PostgreSQL, and a separate report shows them.

DAS 839 NoSQL Systems, end-semester project. The rules every pipeline follows are in [SPEC.md](SPEC.md).

## Status

| Part | Owner | Status |
|---|---|---|
| Controller, batching, loader, report, compare, schema | Aprajita | Built and tested |
| MongoDB pipeline | Navya | Built; identical to the reference on the full data |
| MapReduce pipeline | Jatin (with Saravanan) | Built with Hadoop Streaming, identical to the reference on the full data; being rewritten in Java (parsing must not be in Python files) |
| Pig pipeline | Aprajita | Built; identical to the reference on the full data |
| Hive pipeline | Saravanan | In progress |

## Setup

Python 3.10 or newer.

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Start PostgreSQL. The script runs a private PostgreSQL that lives in `work/pgdata` and listens on `localhost:5433`. It needs PostgreSQL installed (on a Mac: `brew install postgresql@16`):

```bash
scripts/dev_postgres.sh start
```

Then create the tables and download the dataset (about 35 MB compressed):

```bash
.venv/bin/etl init-db
```

```bash
scripts/download_data.sh
```

MongoDB, for the MongoDB pipeline: `scripts/dev_mongodb.sh start`. The first start downloads MongoDB's official build (8.0.38, about 82 MB, macOS) into `work/mongodb`; it then runs on `localhost:27017`. No Homebrew or Docker needed. On Linux or Windows, install MongoDB 8.0 from mongodb.com instead.

```bash
scripts/dev_mongodb.sh start
```

Stop either database with `stop` instead of `start`.

Hadoop, for the MapReduce pipeline (Hadoop 3.x in standalone mode, with Java). Set `HADOOP_HOME` if Hadoop is not in `~/hadoop-tutorial/hadoop-3.4.3`, then load the settings in every new terminal:

```bash
source scripts/mapreduce_env.sh
```

Pig, for the Pig pipeline: downloads Apache Pig 0.18.0 (about 492 MB) into `work/pig` once, after checking Apache's checksum. Pig needs a Java 8 JDK (`javac`) and works with or without a local Hadoop.

```bash
scripts/dev_pig.sh install
```

Settings come from environment variables:

| Variable | Default | Used by |
|---|---|---|
| `ETL_PG_DSN` | `postgresql://etl@localhost:5433/etl` | everything |
| `ETL_MONGO_URI` | `mongodb://localhost:27017` | MongoDB |
| `ETL_DATA_DIR` | `data/` | everything |
| `ETL_WORK_DIR` | `work/` | everything |
| `HADOOP_HOME`, `ETL_HDFS_ROOT`, `ETL_MR_PYTHON` | set by `scripts/mapreduce_env.sh` | MapReduce (and Pig, optionally) |
| `PIG_HOME`, `ETL_PIG_ROOT` | `work/pig`, `work/pig_runs` | Pig |

## Usage

The simplest way: type `etl` on its own. A menu asks for the pipeline, the batch size and the input, runs it, and offers the report.

```bash
.venv/bin/etl
```

The same run without the menu:

```bash
.venv/bin/etl run --pipeline mongodb --batch-size 500000
```

```bash
.venv/bin/etl runs
```

```bash
.venv/bin/etl report --latest
```

```bash
.venv/bin/etl report --latest --batch 3
```

```bash
.venv/bin/etl compare --runs RUN_ID_A RUN_ID_B
```

```bash
.venv/bin/etl verify --run RUN_ID
```

`etl run` cuts each raw file into batches of unchanged lines (a batch never joins the two files) and gives each batch to the selected pipeline. The pipeline parses the batch and runs Q1–Q3 over that batch alone. After the last batch it runs Q1–Q3 over every parsed record. Everything is written to PostgreSQL in one transaction: per-batch results with `batch_id` 1…n, whole-run results with `batch_id` 0. The runtime covers reading the first input file through to that commit.

`etl report` shows the whole-run results by default, or one batch's with `--batch K`. `etl verify` re-reads the raw files and checks each batch's line range and SHA-256, proving the slices were unchanged.

## Adding a pipeline

1. Implement the four methods of `etl.pipeline.Pipeline` in `pipelines/<name>/pipeline.py`: `setup`, `process_batch`, `run_queries`, `cleanup`. Remove the `NotImplementedError` in `__init__`. `run_queries(run_id, batch_id)` is called with each batch ID right after that batch, and with no batch ID once at the end for the whole run, so keep every batch's parsed records until `cleanup`.
2. All parsing and query work must run inside your technology. Python only starts the work and reads results back as the tuples in `etl/pipeline.py`.
3. The controller checks every batch's counts and validates every result row against SPEC.md before loading, so mistakes fail loudly with the row that broke the rule.
4. Check your output against trusted results. `tests/run_reference.py` runs the test-only reference pipeline through the real controller, so you can compare with it:

   ```bash
   .venv/bin/python tests/run_reference.py --input tests/golden/sample.log --batch-size 50
   .venv/bin/etl run --pipeline mongodb --input tests/golden/sample.log --batch-size 50
   .venv/bin/etl compare --runs REFERENCE_RUN_ID MONGODB_RUN_ID
   ```

   Then compare full runs of two real pipelines the same way.

## Tests

```bash
.venv/bin/pytest
```

Each built pipeline also has its own test against the reference, run separately because they need their engine:

```bash
.venv/bin/pytest pipelines/mongodb pipelines/pig
```

The end-to-end tests need PostgreSQL running and use a separate `etl_test` database. They run the whole flow with `tests/reference_pipeline.py`, a plain-Python version of SPEC.md that exists only for testing and for working out expected results; it is not one of the tool's pipelines.
