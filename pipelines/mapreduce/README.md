# MapReduce pipeline

The pipeline uses Java Hadoop MapReduce jobs for parsing and Q1–Q3. The Python
`pipeline.py` only compiles and launches those jobs, copies batch files to the
configured Hadoop filesystem, reads counters and query output, and maps output
columns to the shared pipeline result types.

## Java jobs

`java/nasa/etl/mapreduce/MapReduceJobs.java` contains:

| Job | Work |
|---|---|
| `parse` | Match the SPEC line and request patterns, emit parsed records, and count records, parsed lines, and malformed lines |
| `q1` | Group by date and status; sum request counts and bytes (with a combiner) |
| `q2-aggregate` | Group by resource path; sum requests and bytes and count distinct hosts |
| `q2-top20` | Rank by requests descending, then the original path's character code ascending; emit at most 20 |
| `q3` | Group by date and hour; calculate error counts, exact double rate, and distinct error hosts |

Both Java regular expressions begin with `(?d)`. The parse mapper decodes the
Hadoop `Text` bytes as ISO-8859-1, removes one trailing carriage return, and
uses the counters `etl_records`, `etl_parsed`, and `etl_malformed`. Internal
records escape backslashes, tabs, line breaks, and non-ASCII/control characters,
so a tab-containing path or a garbage request containing control bytes cannot
corrupt Hadoop's tab-delimited text files. Values are restored when Q2 results
are read back.

## Setup and execution

Follow these steps in order. Run commands from the **repository root**: the
directory containing `pyproject.toml`, `scripts/`, `data/`, and
`pipelines/`. Do not run them from `pipelines/mapreduce/`.

### 1. Create the Python environment (once)

This installs the `etl` command into `.venv`. Without it,
`.venv/bin/etl` will not exist.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
```

### 2. Start PostgreSQL and create the project tables

The ETL runner stores run details and query results in PostgreSQL. If the
PostgreSQL command-line tools must be installed first (on macOS, the root
README suggests `brew install postgresql@16`). If the project's development
script is not executable on your machine, invoke it with `bash` as shown:

```bash
bash scripts/dev_postgres.sh start
.venv/bin/etl init-db
```

### 3. Download the NASA input files (once)

The default run expects both decompressed files in `data/`. They are not
included in the repository. Use `bash` so this works even if the script does
not have its executable bit set:

```bash
bash scripts/download_data.sh
```

For a quick smoke test instead of the full dataset, use the included sample
with `--input tests/golden/sample.log` (see step 5).

### 4. Configure Hadoop in each new terminal

You need a Java JDK (including `javac`) and Hadoop 3.x installed. Set
`HADOOP_HOME` to the **actual Hadoop installation directory**: it must contain
`bin/hadoop`. Do not copy `/path/to/hadoop` literally; that is only a
placeholder. For example, if Hadoop is installed at `$HOME/hadoop`:

```bash
export HADOOP_HOME="$HOME/hadoop"
source scripts/mapreduce_env.sh
"$HADOOP_HOME/bin/hadoop" version
```

If your Hadoop installation is elsewhere, replace `$HOME/hadoop` with its real
path. The settings script must be sourced from the repository root, not run as
a child process. It sets `ETL_HDFS_ROOT` to
`file://$PWD/work/etl_runs` by default, which makes Hadoop use local files in
standalone mode and keeps generated files under the ignored `work/` directory.
You do not need to start HDFS for this mode. If `HADOOP_HOME` or
`ETL_HDFS_ROOT` was already exported to an incorrect value, correct or unset it
before sourcing the script; existing values are preserved.

The script also sets `ETL_MR_PYTHON` for the older Streaming setup; the current
Java MapReduce jobs do not use that variable.

### 5. Run MapReduce

From the repository root, with PostgreSQL running, tables initialized, input
files present, and Hadoop configured:

```bash
.venv/bin/etl run --pipeline mapreduce --batch-size 500000
```

For the golden sample:

```bash
.venv/bin/etl run --pipeline mapreduce --input tests/golden/sample.log --batch-size 50
```

The Java sources are compiled against `hadoop classpath --glob` when the
pipeline is constructed. Temporary compiled classes are removed by
`cleanup()`. `ETL_HADOOP_BIN` can override the Hadoop executable if it is not
at `$HADOOP_HOME/bin/hadoop`.

After a run, check that batches are unchanged slices of the raw files and,
when a reference run exists, compare every query result:

```bash
.venv/bin/etl verify --run RUN_ID
.venv/bin/etl compare --runs REFERENCE_RUN_ID RUN_ID
```

Every batch is parsed independently and retained under
`<ETL_HDFS_ROOT>/<run_id>/parsed/batch_<k>`. Per-batch queries use only that
folder; whole-run queries read all `parsed/batch_*` folders directly, not the
previously computed batch answers.
