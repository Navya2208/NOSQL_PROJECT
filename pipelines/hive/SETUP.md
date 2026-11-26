# Hive Setup and Execution

## Tested Environment

- OS: WSL Ubuntu
- Java: 21
- Hadoop: 3.4.3
- Hive: 4.2.1
- Python: 3.14.4
- Hive JDBC URL: jdbc:hive2://127.0.0.1:10000/nasa_logs

## Environment Variables

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HIVE_HOME="$HOME/apache-hive-4.2.1-bin"
export HDFS_BIN="$HADOOP_HOME/bin/hdfs"
export ETL_HIVE_URL="jdbc:hive2://127.0.0.1:10000/nasa_logs"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH"

## Start HDFS

start-dfs.sh

Check services:

jps

## Start HiveServer2

export HADOOP_OPTS="-Xmx2g"

nohup "$HIVE_HOME/bin/hiveserver2" \
  --hiveconf javax.jdo.option.ConnectionURL="jdbc:derby:$HOME/metastore_db;create=true" \
  --hiveconf hive.aux.jars.path="file://$HIVE_HOME/lib/commons-collections-3.2.2.jar,file://$PWD/pipelines/hive/build/latin1-serde.jar" \
  --hiveconf hive.execution.engine=mr \
  --hiveconf hive.server2.tez.initialize.default.sessions=false \
  --hiveconf hive.server2.tez.sessions.per.default.queue=0 \
  --hiveconf hive.query.results.cache.enabled=false \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \
  > /tmp/saravanan/hive.log 2>&1 &

## Test Hive Connection

beeline -u "$ETL_HIVE_URL" -e "SELECT 1"

## Run Hive Pipeline

ETL_HIVE_KEEP=1 .venv/bin/etl run --pipeline hive --batch-size 500000
