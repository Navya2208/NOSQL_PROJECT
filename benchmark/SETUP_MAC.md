# Benchmark machine setup (Mac, Homebrew)

Owner: Navya. This is the setup that produced the final timings on 26–27 November 2026: a MacBook with Apple Silicon, MongoDB 8.0, PostgreSQL 16, Java 17, Hadoop 3.5.0, Pig 0.18.0 and Hive 4.2.1. All four pipelines were IDENTICAL to the reference on the full data. Results are in `evidence/benchmark/`.

## 1. Install (once)

```bash
brew tap mongodb/brew && brew trust mongodb/brew
brew install python@3.12 postgresql@16 mongodb-community@8.0 hadoop hive gh
brew services start mongodb-community@8.0

python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
bash scripts/download_data.sh
bash scripts/dev_pig.sh install        # Pig 0.18.0 into work/pig
```

## 2. Settings for every terminal

Save this once as `~/nosql_env.sh`, then run `source ~/nosql_env.sh` in every new terminal. Without it, Terminal may use an older Java (for example, Anaconda's Java 8), which Hadoop 3.5 rejects.

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export HADOOP_HOME="$(brew --prefix hadoop)/libexec"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HIVE_HOME="$(brew --prefix hive)/libexec"
export HDFS_BIN="$HADOOP_HOME/bin/hdfs"
export ETL_HIVE_URL="jdbc:hive2://127.0.0.1:10000/nasa_logs"
export PG_BIN="$(brew --prefix postgresql@16)/bin"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
cd ~/Desktop/5/nosql_project/final_repo     # the repository folder
source scripts/mapreduce_env.sh
```

## 3. PostgreSQL

```bash
bash scripts/dev_postgres.sh start
.venv/bin/etl init-db
```

If it reports the port is in use, another copy of the project's database is running. Stop it with `kill $(lsof -t -iTCP:5433 -sTCP:LISTEN)`.

## 4. HDFS for Hive (single node)

MongoDB, MapReduce and Pig do not need HDFS. MapReduce uses local files (`ETL_HDFS_ROOT=file://...`), and Pig runs in local mode. Hive needs HDFS.

Port 9000 was taken by another program on this Mac, so HDFS uses port 9820. Write the settings once:

```bash
mkdir -p ~/hadoop-data/name ~/hadoop-data/data
cat > "$HADOOP_CONF_DIR/core-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9820</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$HOME/hadoop-data/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$HOME/hadoop-data/data</value></property>
</configuration>
EOF
hdfs namenode -format -nonInteractive
```

Start HDFS after every restart. The `hdfs --daemon` commands need no SSH:

```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs dfs -mkdir -p /tmp && hdfs dfs -chmod 1777 /tmp
jps                                      # NameNode and DataNode
```

## 5. HiveServer2

Create the metastore once:

```bash
schematool -dbType derby -initSchema -url "jdbc:derby:$HOME/metastore_db;create=true"
```

Start HiveServer2 after every restart, from the repository folder. Two settings differ from Saravanan's Linux setup:

- `hive.server2.enable.doAs=false`. Without it, Hadoop refuses with "User ... is not allowed to impersonate anonymous".
- A 4 GB heap. With the default, the in-process MapReduce step fails with `OutOfMemoryError: Java heap space`.

```bash
export HADOOP_HEAPSIZE=4096 HADOOP_OPTS="-Xmx4g" HADOOP_CLIENT_OPTS="-Xmx4g"
nohup hiveserver2 \
  --hiveconf javax.jdo.option.ConnectionURL="jdbc:derby:$HOME/metastore_db;create=true" \
  --hiveconf hive.aux.jars.path="file://$HIVE_HOME/lib/commons-collections-3.2.2.jar,file://$PWD/pipelines/hive/build/latin1-serde.jar" \
  --hiveconf hive.execution.engine=mr \
  --hiveconf hive.server2.tez.initialize.default.sessions=false \
  --hiveconf hive.server2.tez.sessions.per.default.queue=0 \
  --hiveconf hive.query.results.cache.enabled=false \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \
  --hiveconf hive.server2.enable.doAs=false \
  > /tmp/hive.log 2>&1 &
```

After about a minute, create the database once and test the connection:

```bash
beeline -u "jdbc:hive2://127.0.0.1:10000/default" -e "CREATE DATABASE IF NOT EXISTS nasa_logs"
beeline -u "$ETL_HIVE_URL" -e "SELECT 1"
```

Hive's detailed log on a Mac is `$TMPDIR/$USER/hive.log`, not `/tmp`.

## 6. Checks and benchmark

```bash
.venv/bin/pytest                                   # 33 passed
.venv/bin/pytest pipelines/mongodb pipelines/pig   # 5 passed

# Golden sample, any pipeline against the reference
.venv/bin/python tests/run_reference.py --input tests/golden/sample.log --batch-size 50
.venv/bin/etl run --pipeline <name> --input tests/golden/sample.log --batch-size 50
.venv/bin/etl compare --runs <reference_id> <run_id>

# Full-data benchmark: same batch size for every pipeline; MongoDB included so each pipeline is also compared with it
caffeinate -i .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb pig --batch-sizes 500000 --repeats 3
caffeinate -i .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb mapreduce --batch-sizes 500000 --repeats 3
caffeinate -i .venv/bin/python benchmark/run_benchmark.py --pipelines mongodb hive --batch-sizes 500000 --repeats 1
```

`caffeinate -i` keeps the Mac awake during a run. Each benchmark writes a CSV and a Markdown summary to `benchmark/results/`. Copy the final ones to `evidence/benchmark/` for the report.

## Final results (batch size 500,000; 3,461,613 records, 1 malformed, 8 batches)

| Pipeline | Median runtime, s (min–max) | Runs | vs reference | vs MongoDB |
|---|---|---|---|---|
| Reference (Python, test only) | 15.0 | 1 | — | — |
| MongoDB | 82.1 (67.1–116.5) | 7 | IDENTICAL | — |
| MapReduce | 125.4 (122.2–125.9) | 3 | IDENTICAL | IDENTICAL |
| Pig | 163.7 (148.7–174.2) | 3 | IDENTICAL | IDENTICAL |
| Hive | 568.6 (383.2–593.0) | 3 | IDENTICAL | IDENTICAL |

`etl verify` matched every batch of every run.

### Batch size 100,000 (35 batches, one run each)

| Pipeline | Runtime, s | At 500,000 (median), s | vs reference | vs MongoDB |
|---|---|---|---|---|
| MongoDB | 74.2 | 82.1 | IDENTICAL | — |
| MapReduce | 421.9 | 125.4 | IDENTICAL | IDENTICAL |
| Pig | 347.5 | 163.7 | IDENTICAL | IDENTICAL |
| Hive | 1,319.4 | 568.6 | IDENTICAL | IDENTICAL |

Smaller batches barely change MongoDB, whose cost is per record, but slow the Hadoop-based engines 2–3.4×, because each batch starts fresh jobs (JVM start-up, job setup) for parsing and for Q1–Q3.
