"""Pig pipeline. Owner: Aprajita.

All parsing, malformed detection, counting and query work runs in Pig Latin:
  parse.pig     one batch: Latin-1 load, REGEX_EXTRACT_ALL, SPLIT parsed / malformed,
                field derivation, BinStorage output, record counts
  queries.pig   Q1, Q2, Q3 over one batch's parsed records or all of them (whole run)

Python here only builds the small Latin-1 loader jar once, starts Pig, and reads the
JSON results back into the tuples defined in etl/pipeline.py. It does no parsing,
arithmetic or sorting.

Settings (environment variables):
  PIG_HOME       Pig install, default work/pig (scripts/dev_pig.sh install)
  HADOOP_HOME    Hadoop install, used for the loader's classpath (scripts/mapreduce_env.sh)
  ETL_PIG_ROOT   folder for run data, default work/pig_runs
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

from etl import config
from etl.pipeline import BatchResult, Pipeline, QueryResults

HERE = Path(__file__).resolve().parent
LOADER_SRC = HERE / "java" / "nasa" / "etl" / "Latin1TextLoader.java"


def pig_home() -> Path:
    return Path(os.environ.get("PIG_HOME", config.ROOT / "work" / "pig"))


def runs_root() -> Path:
    return Path(os.environ.get("ETL_PIG_ROOT", config.work_dir() / "pig_runs"))


def _classpath(pig: Path) -> str:
    jars = sorted(str(p) for p in pig.glob("pig-*-core-h3.jar")) or sorted(str(p) for p in pig.glob("pig-*.jar"))
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        done = subprocess.run([str(Path(hadoop_home) / "bin" / "hadoop"), "classpath"],
                              capture_output=True, text=True)
        if done.returncode == 0:
            jars.append(done.stdout.strip())
    else:
        jars += sorted(str(p) for p in (pig / "lib").rglob("hadoop-*.jar"))
    return os.pathsep.join(jars)


def build_loader(pig: Path) -> Path:
    """Compiles Latin1TextLoader.java into work/pig_udf/latin1-loader.jar when it is missing or older."""
    out_dir = config.work_dir() / "pig_udf"
    jar = out_dir / "latin1-loader.jar"
    if jar.exists() and jar.stat().st_mtime >= LOADER_SRC.stat().st_mtime:
        return jar
    classes = out_dir / "classes"
    shutil.rmtree(classes, ignore_errors=True)
    classes.mkdir(parents=True)
    done = subprocess.run(["javac", "-source", "8", "-target", "8", "-nowarn", "-cp", _classpath(pig),
                           "-d", str(classes), str(LOADER_SRC)], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"could not compile the Latin-1 loader:\n{done.stderr[-3000:]}")
    done = subprocess.run(["jar", "cf", str(jar), "-C", str(classes), "."], capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"could not package the Latin-1 loader:\n{done.stderr[-3000:]}")
    return jar


def _read_json_rows(folder: Path) -> list[dict]:
    """Rows of a JsonStorage output, in part-file order (Q2 is already ranked by Pig)."""
    rows = []
    for part in sorted(folder.glob("part-*")):
        with open(part, encoding="utf-8") as f:
            rows += [json.loads(line) for line in f if line.strip()]
    return rows


class PigPipeline(Pipeline):
    name = "pig"

    def __init__(self):
        home = pig_home()
        self.pig = home / "bin" / "pig"
        if not self.pig.exists():
            raise RuntimeError(f"Pig not found at {self.pig}. Install it with: scripts/dev_pig.sh install")
        self.loader_jar = build_loader(home)
        # Pig 0.18.0 ships these libraries but leaves them off its own classpath:
        #  - Commons Collections 3: needed by its reduce phase; Hadoop 3.4 no longer includes it.
        #  - Commons Lang 3, Compress and Text: needed when no Hadoop install is found and Pig
        #    runs on its bundled Hadoop (for example a fresh machine without HADOOP_HOME).
        # Add Pig's own copies. Pig appends PIG_CLASSPATH after Hadoop's jars, so a local
        # Hadoop's newer versions still win where they exist.
        self.env = dict(os.environ)
        lib = home / "lib"
        extra = [str(p) for p in sorted((lib / "hadoop3-runtime").glob("commons-collections-3*.jar"))]
        for name in ("commons-lang3-*.jar", "commons-compress-*.jar", "commons-text-*.jar"):
            extra += [str(p) for p in sorted(lib.rglob(name))][:1]
        if extra:
            self.env["PIG_CLASSPATH"] = os.pathsep.join(filter(None, [self.env.get("PIG_CLASSPATH"), *extra]))

    # ---- plumbing ------------------------------------------------------------

    def _run_dir(self, run_id: str) -> Path:
        return runs_root() / run_id

    def _pig(self, run_id: str, script: str, label: str, **params: str) -> None:
        log = self._run_dir(run_id) / "logs" / f"{label}.log"
        cmd = [str(self.pig), "-x", "local", "-logfile", str(log)]
        for key, value in params.items():
            cmd += ["-param", f"{key}={value}"]
        cmd += ["-f", str(HERE / script)]
        done = subprocess.run(cmd, capture_output=True, text=True, cwd=self._run_dir(run_id), env=self.env)
        if done.returncode != 0:
            lines = (done.stderr or "").splitlines()
            causes = [l for l in lines if "Exception" in l or "Caused by" in l][:6]
            tail = "\n".join(causes + ["..."] + lines[-15:])
            raise RuntimeError(f"Pig script {script} ({label}) failed (exit {done.returncode}):\n{tail}")

    # ---- the four pipeline steps -----------------------------------------------

    def setup(self, run_id: str) -> None:
        run = self._run_dir(run_id)
        shutil.rmtree(run, ignore_errors=True)
        for sub in ("parsed", "counts", "out", "logs"):
            (run / sub).mkdir(parents=True)

    def process_batch(self, run_id: str, batch_id: int, batch_path: str) -> BatchResult:
        run = self._run_dir(run_id)
        self._pig(run_id, "parse.pig", f"parse_b{batch_id}",
                  loader_jar=str(self.loader_jar),
                  input=str(Path(batch_path).resolve()),
                  parsed=str(run / "parsed" / f"batch_{batch_id}"),
                  counts=str(run / "counts" / f"batch_{batch_id}"))
        counts = _read_json_rows(run / "counts" / f"batch_{batch_id}")
        if len(counts) != 1:
            raise RuntimeError(f"batch {batch_id}: expected one counts row from Pig, got {counts}")
        c = counts[0]
        return BatchResult(int(c["records"]), int(c["parsed"]), int(c["malformed"]))

    def run_queries(self, run_id: str, batch_id: int | None = None) -> QueryResults:
        run = self._run_dir(run_id)
        if batch_id is None:
            source, scope = str(run / "parsed" / "batch_*"), "all"
        else:
            source, scope = str(run / "parsed" / f"batch_{batch_id}"), f"b{batch_id}"
        out = run / "out" / scope
        shutil.rmtree(out, ignore_errors=True)
        self._pig(run_id, "queries.pig", f"queries_{scope}", input=source, out=str(out))

        q1 = [(r["log_date"], int(r["status_code"]), int(r["request_count"]), int(r["total_bytes"]))
              for r in _read_json_rows(out / "q1")]
        q2 = [(r["resource_path"] if r["resource_path"] is not None else "", int(r["request_count"]),
               int(r["total_bytes"]), int(r["distinct_host_count"]))
              for r in _read_json_rows(out / "q2")]
        q3 = [(r["log_date"], int(r["log_hour"]), int(r["error_request_count"]),
               int(r["total_request_count"]), float(r["error_rate"]), int(r["distinct_error_hosts"]))
              for r in _read_json_rows(out / "q3")]
        return QueryResults(q1=q1, q2=q2, q3=q3)

    def cleanup(self, run_id: str) -> None:
        if os.environ.get("ETL_PIG_KEEP"):  # keep run data for inspection
            return
        shutil.rmtree(self._run_dir(run_id), ignore_errors=True)
