# Settings the MapReduce pipeline needs. Load them into the current terminal with:
#
#     source scripts/mapreduce_env.sh
#
# Run it from the project folder. If Hadoop is installed somewhere else, set
# HADOOP_HOME first. Hadoop runs in standalone (local) mode on local files.

if [ ! -f pyproject.toml ] || [ ! -d pipelines/mapreduce ]; then
  echo "Run this from the project folder: source scripts/mapreduce_env.sh"
  return 1 2>/dev/null || exit 1
fi

export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-tutorial/hadoop-3.4.3}"
export ETL_HDFS_ROOT="${ETL_HDFS_ROOT:-file://$PWD/work/etl_runs}"
export ETL_MR_PYTHON="${ETL_MR_PYTHON:-$PWD/.venv/bin/python}"

if [ -x "$HADOOP_HOME/bin/hadoop" ]; then
  echo "MapReduce settings loaded: HADOOP_HOME=$HADOOP_HOME"
else
  echo "Warning: no Hadoop found at $HADOOP_HOME/bin/hadoop. Set HADOOP_HOME and source this again."
fi
