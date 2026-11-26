# Benchmark machine setup (Mac, Homebrew)

Owner: Navya. The benchmark machine must run all four pipelines, so it needs MongoDB, PostgreSQL, Java, Hadoop, Pig and Hive. MongoDB and PostgreSQL are already set up (see `CLAUDE.md`).

This guide gets the engines installed and checked. Each pipeline's own settings (Hive metastore, Pig mode, how the MapReduce jar is built) come from that pipeline's owner: Jatin (MapReduce), Aprajita (Pig), Saravanan (Hive). Add their steps here as they arrive, so the final runs use the same setup as development.

## 1. Install

All three are in Homebrew, as checked on 23 November 2026:

| Formula | Version | Depends on |
|---|---|---|
| `hadoop` | 3.5.0 | `openjdk@17` |
| `pig` | 0.18.0 | `openjdk@21` |
| `hive` | 4.2.1 | `hadoop`, `openjdk@21` |

```bash
brew install hadoop pig hive
```

Homebrew installs the Java versions each one needs. Installing takes several minutes.

## 2. Check each engine starts

```bash
hadoop version
pig -version
hive --version
java -version
javac -version
```

Each should print a version and no error. `javac` is needed to compile the Java mapper for MapReduce. If `java` or `javac` is missing, run `brew install openjdk@17` and follow the "caveats" lines Homebrew prints.

## 3. Quick smoke tests (local mode, no cluster)

Hadoop's local mode runs jobs as one process on the laptop, with no HDFS daemons to start. That is enough for the benchmark unless a pipeline owner says their pipeline needs HDFS.

```bash
# Hadoop: run the built-in word-count example on a tiny file
mkdir -p /tmp/hadoop-smoke && echo "a b a" > /tmp/hadoop-smoke/in.txt
hadoop jar "$(brew --prefix hadoop)"/libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  wordcount /tmp/hadoop-smoke/in.txt /tmp/hadoop-smoke/out
cat /tmp/hadoop-smoke/out/part-r-00000      # expect: a 2, b 1

# Pig: local mode
echo "lines = LOAD '/tmp/hadoop-smoke/in.txt' AS (l:chararray); DUMP lines;" > /tmp/smoke.pig
pig -x local /tmp/smoke.pig                  # expect: (a b a)
```

Hive needs a metastore (the catalogue of tables) before its first query. Use whatever Saravanan uses for the Hive pipeline, so both machines behave the same. Note his commands here.

## 4. Run the benchmark

With MongoDB and PostgreSQL running (`brew services list`, `bash scripts/dev_postgres.sh start`):

```bash
# MongoDB alone, three batch sizes
.venv/bin/python benchmark/run_benchmark.py --pipelines mongodb --batch-sizes 100000 500000 1000000

# All four, once they are built: same batch size, three repeats each
.venv/bin/python benchmark/run_benchmark.py --pipelines mongodb mapreduce pig hive --batch-sizes 500000 --repeats 3
```

Each run writes `benchmark/results/benchmark_<time>.csv` (one row per run) and a `.md` summary table for the report. A pipeline that is not built yet, or whose engine is not running, is recorded as failed and the benchmark carries on with the rest.

For fair timings, close other heavy apps, keep the laptop plugged in, and use the same batch size for every pipeline.
