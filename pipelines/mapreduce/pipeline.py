"""Hadoop MapReduce pipeline launcher. Parsing and aggregation run in Java jobs."""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from etl.pipeline import BatchResult, Pipeline, QueryResults


HERE = Path(__file__).resolve().parent
JAVA_SOURCE = HERE / "java" / "nasa" / "etl" / "mapreduce" / "MapReduceJobs.java"


def _hadoop_bin() -> str:
    if os.environ.get("ETL_HADOOP_BIN"):
        return os.environ["ETL_HADOOP_BIN"]
    home = os.environ.get("HADOOP_HOME")
    return str(Path(home) / "bin" / "hadoop") if home else "hadoop"


def _streaming_root() -> str:
    return os.environ.get("ETL_HDFS_ROOT", "etl_runs").rstrip("/")


def _unescape(value: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(value):
        if value[index] != "\\":
            output.append(value[index])
            index += 1
        elif value.startswith("\\\\", index):
            output.append("\\")
            index += 2
        elif index + 3 < len(value) and value[index + 1] == "x":
            output.append(chr(int(value[index + 2:index + 4], 16)))
            index += 4
        else:
            raise ValueError(f"invalid escaped field from MapReduce output: {value!r}")
    return "".join(output)


class MapReducePipeline(Pipeline):
    name = "mapreduce"

    def __init__(self) -> None:
        self.hadoop = _hadoop_bin()
        self._build_dir = Path(tempfile.mkdtemp(prefix="etl-mapreduce-java-"))
        self._jar = self._build_dir / "mapreduce-jobs.jar"
        try:
            self._compile()
        except Exception:
            shutil.rmtree(self._build_dir)
            raise

    def _run(self, args: list[str], operation: str) -> subprocess.CompletedProcess[bytes]:
        done = subprocess.run(args, capture_output=True, check=False)
        if done.returncode:
            detail = (done.stderr or done.stdout).decode("utf-8", "replace")[-4000:]
            raise RuntimeError(f"{operation} failed (exit {done.returncode}):\n{detail}")
        return done

    def _compile(self) -> None:
        classpath = self._run(
            [self.hadoop, "classpath", "--glob"], "getting Hadoop classpath"
        ).stdout.decode("utf-8").strip()
        classes = self._build_dir / "classes"
        classes.mkdir()
        self._run(
            ["javac", "-encoding", "UTF-8", "-cp", classpath, "-d", str(classes), str(JAVA_SOURCE)],
            "compiling Java MapReduce jobs",
        )
        self._run(["jar", "cf", str(self._jar), "-C", str(classes), "."], "packaging Java MapReduce jobs")

    def _hadoop(self, *args: str, operation: str) -> subprocess.CompletedProcess[bytes]:
        return self._run([self.hadoop, *args], operation)

    def _job(self, *args: str, operation: str) -> str:
        done = self._hadoop("jar", str(self._jar), "nasa.etl.mapreduce.MapReduceJobs", *args,
                            operation=operation)
        return done.stdout.decode("utf-8", "replace")

    @staticmethod
    def _run_dir(run_id: str) -> str:
        return f"{_streaming_root()}/{run_id}"

    @classmethod
    def _parsed_dir(cls, run_id: str, batch_id: int) -> str:
        return f"{cls._run_dir(run_id)}/parsed/batch_{batch_id}"

    def setup(self, run_id: str) -> None:
        root = self._run_dir(run_id)
        self._hadoop("fs", "-rm", "-r", "-f", root, operation=f"clearing run {run_id}")
        self._hadoop("fs", "-mkdir", "-p", f"{root}/input", f"{root}/parsed", f"{root}/out",
                     operation=f"creating run folders for {run_id}")

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        input_path = f"{self._run_dir(run_id)}/input/batch_{batch_id}.log"
        output_path = self._parsed_dir(run_id, batch_id)
        self._hadoop("fs", "-put", "-f", str(Path(batch_path).resolve()), input_path,
                     operation=f"copying batch {batch_id} to Hadoop")
        job_output = self._job("parse", input_path, output_path, operation=f"parsing batch {batch_id}")
        counter_lines = [line for line in job_output.splitlines() if line.startswith("ETL_COUNTERS ")]
        values = counter_lines[-1].split()[1:] if counter_lines else []
        if len(values) != 3:
            raise RuntimeError(f"Java parse job returned invalid counters for batch {batch_id}: {values!r}")
        records, parsed, malformed = map(int, values)
        return BatchResult(record_count=records, parsed_count=parsed, malformed_count=malformed)

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        if batch_id is None:
            input_path = f"{self._run_dir(run_id)}/parsed/batch_*"
            scope = "all"
        else:
            input_path = self._parsed_dir(run_id, batch_id)
            scope = f"b{batch_id}"

        output_root = f"{self._run_dir(run_id)}/out/{scope}"
        q1_path, q2_agg_path = f"{output_root}_q1", f"{output_root}_q2agg"
        q2_path, q3_path = f"{output_root}_q2", f"{output_root}_q3"

        self._job("q1", input_path, q1_path, operation=f"running Q1 ({scope})")
        self._job("q2-aggregate", input_path, q2_agg_path, operation=f"running Q2 aggregation ({scope})")
        self._job("q2-top20", q2_agg_path, q2_path, operation=f"ranking Q2 ({scope})")
        self._job("q3", input_path, q3_path, operation=f"running Q3 ({scope})")

        q1 = []
        for line in self._read_output(q1_path):
            log_date, status, count, total_bytes = line.split("\t")
            q1.append((log_date, int(status), int(count), int(total_bytes)))

        q2 = []
        for line in self._read_output(q2_path):
            escaped_path, count, total_bytes, host_count = line.split("\t")
            q2.append((_unescape(escaped_path), int(count), int(total_bytes), int(host_count)))

        q3 = []
        for line in self._read_output(q3_path):
            log_date, hour, errors, total, rate, hosts = line.split("\t")
            q3.append((log_date, int(hour), int(errors), int(total), float(rate), int(hosts)))
        return QueryResults(q1=q1, q2=q2, q3=q3)

    def _read_output(self, output_path: str) -> list[str]:
        result = self._hadoop("fs", "-cat", f"{output_path}/part-*",
                              operation=f"reading Hadoop output {output_path}")
        return [line for line in result.stdout.decode("ascii").splitlines() if line]

    def cleanup(self, run_id: str) -> None:
        try:
            self._hadoop("fs", "-rm", "-r", "-f", self._run_dir(run_id),
                         operation=f"removing Hadoop work for {run_id}")
        finally:
            if self._build_dir.exists():
                shutil.rmtree(self._build_dir)
