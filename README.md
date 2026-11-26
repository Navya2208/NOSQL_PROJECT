# NASA Log Multi-Pipeline ETL

One tool that runs the same ETL and analytics workload on the NASA HTTP web server logs with a choice of four backends: **MongoDB**, **MapReduce**, **Pig** or **Hive**. Results and run metadata are loaded into PostgreSQL, and a separate report shows them.

DAS 839 NoSQL Systems, end-semester project. The rules every pipeline follows are in [SPEC.md](SPEC.md).

## Status

| Part | Owner | Status |
|---|---|---|
| Controller, batching, loader, report, compare, schema | Aprajita | Built and tested |
| MongoDB pipeline | Navya | Built; identical to the reference on the full data |
| MapReduce pipeline | Jatin (with Saravanan) | Built with Java MapReduce jobs; identical to the reference on the golden sample and the full data |
| Pig pipeline | Aprajita | Built; identical to the reference on the full data |
| Hive pipeline | Saravanan | Built and tested; identical to the reference on the full data |

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
scripts/download_data.sh
```

MongoDB, for the MongoDB pipeline: `scripts/dev_mongodb.sh start`. The first start downloads MongoDB's official build (8.0.38, about 82 MB, macOS) into `work/mongodb`; it then runs on `localhost:27017`. No Homebrew or Docker needed. On Linux or Windows, install MongoDB 8.0 from mongodb.com instead.

```bash
scripts/dev_mongodb.sh start
```

Stop either database with `stop` instead of `start`.

### Hadoop and MapReduce

Hadoop, for the MapReduce pipeline (Hadoop 3.x in standalone mode, with Java). Set `HADOOP_HOME` if Hadoop is not in `~/hadoop-tutorial/hadoop-3.4.3`, then load the settings in every new terminal:

```bash
source scripts/mapreduce_env.sh
```

### Pig

Pig, for the Pig pipeline: downloads Apache Pig 0.18.0 (about 492 MB) into `work/pig` once, after checking Apache's checksum. Pig needs a Java 8 JDK (`javac`) and works with or without a local Hadoop.

```bash
scripts/dev_pig.sh install
```

### Hive

Hive, for the Hive pipeline, requires Hadoop, Java and Apache Hive 4.x. The tested setup uses Hadoop 3.4.3, Hive 4.2.1 and Java 21 in WSL.

Set the environment variables in every new terminal. Adjust the paths if Hadoop or Hive is installed elsewhere:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HIVE_HOME="$HOME/apache-hive-4.2.1-bin"
export HDFS_BIN="$HADOOP_HOME/bin/hdfs"
export ETL_HIVE_URL="jdbc:hive2://127.0.0.1:10000/nasa_logs"

export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH"
```

Start HDFS:

```bash
start-dfs.sh
```

Check that the Hadoop daemons are running:

```bash
jps
```

Start HiveServer2 after HDFS is available. From the project root:

```bash
nohup "$HIVE_HOME/bin/hiveserver2" \
  --hiveconf javax.jdo.option.ConnectionURL="jdbc:derby:$HOME/metastore_db;create=true" \
  --hiveconf hive.aux.jars.path="file://$HIVE_HOME/lib/commons-collections-3.2.2.jar,file://$PWD/pipelines/hive/build/latin1-serde.jar" \
  --hiveconf hive.execution.engine=mr \
  --hiveconf hive.server2.tez.initialize.default.sessions=false \
  --hiveconf hive.server2.tez.sessions.per.default.queue=0 \
  --hiveconf hive.query.results.cache.enabled=false \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \
  > /tmp/hive.log 2>&1 &
```

Check the HiveServer2 connection:

```bash
beeline -u "$ETL_HIVE_URL" -e "SELECT 1"
```

The Hive pipeline can then be run through the ETL CLI. To retain Hive tables and intermediate data for inspection, set `ETL_HIVE_KEEP=1` before running.

```bash
ETL_HIVE_KEEP=1 .venv/bin/etl run --pipeline hive --batch-size 500000
```

### Environment variables

Settings come from environment variables:

| Variable | Default | Used by |
|---|---|---|
| `ETL_PG_DSN` | `postgresql://etl@localhost:5433/etl` | Everything |
| `ETL_MONGO_URI` | `mongodb://localhost:27017` | MongoDB |
| `ETL_DATA_DIR` | `data/` | Everything |
| `ETL_WORK_DIR` | `work/` | Everything |
| `HADOOP_HOME`, `ETL_HDFS_ROOT`, `ETL_MR_PYTHON` | Set by `scripts/mapreduce_env.sh` | MapReduce (and Pig, optionally) |
| `PIG_HOME`, `ETL_PIG_ROOT` | `work/pig`, `work/pig_runs` | Pig |
| `HIVE_HOME` | Installation-specific | Hive |
| `HADOOP_HOME` | Installation-specific | Hive and MapReduce |
| `HDFS_BIN` | `$HADOOP_HOME/bin/hdfs` | Hive |
| `ETL_HIVE_URL` | `jdbc:hive2://127.0.0.1:10000/nasa_logs` | Hive |

## Usage

The simplest way: type `etl` on its own. A menu asks for the pipeline, the batch size and the input, runs it, and offers the report.

```bash
.venv/bin/etl
```

The same run without the menu:

```bash
.venv/bin/etl run --pipeline mongodb --batch-size 500000
```

Run the Hive pipeline:

```bash
ETL_HIVE_KEEP=1 .venv/bin/etl run --pipeline hive --batch-size 500000
```

List runs:

```bash
.venv/bin/etl runs
```

Show the latest report:

```bash
.venv/bin/etl report --latest
```

Show one batch's report:

```bash
.venv/bin/etl report --latest --batch 3
```

Compare two runs:

```bash
.venv/bin/etl compare --runs RUN_ID_A RUN_ID_B
```

Verify a run:

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