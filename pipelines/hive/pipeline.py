
from __future__ import annotations

import errno
import os
import pty
import shutil
import subprocess
import tempfile
from pathlib import Path

from etl import config
from etl.pipeline import BatchResult, Pipeline, QueryResults


HERE = Path(__file__).resolve().parent
SQL_DIR = HERE / "sql"
SERDE_JAR = HERE / "build" / "latin1-serde.jar"


def hive_url() -> str:
    return os.environ.get(
        "ETL_HIVE_URL",
        "jdbc:hive2://127.0.0.1:10000/nasa_logs",
    )


def hive_root() -> str:
    return os.environ.get(
        "ETL_HIVE_ROOT",
        "/tmp/nasa_etl_hive_runs",
    ).rstrip("/")


def runs_root() -> Path:
    return Path(
        os.environ.get(
            "ETL_HIVE_LOCAL_ROOT",
            str(config.work_dir() / "hive_runs"),
        )
    )


def safe_run_id(run_id: str) -> str:
    safe = run_id.replace("-", "_")

    if not safe or not safe.replace("_", "").isalnum():
        raise ValueError(f"Invalid run ID: {run_id!r}")

    return safe


class HivePipeline(Pipeline):
    name = "hive"

    def __init__(self) -> None:
        if not SERDE_JAR.is_file():
            raise FileNotFoundError(
                f"Latin-1 SerDe JAR not found: {SERDE_JAR}"
            )

        self.beeline = os.environ.get("BEELINE", "beeline")

        self.hdfs = os.environ.get(
            "HDFS_BIN",
            str(
                Path(
                    os.environ.get(
                        "HADOOP_HOME",
                        "/home/saravanan/hadoop",
                    )
                )
                / "bin"
                / "hdfs"
            ),
        )

        self.run_id: str | None = None
        self.raw_table: str | None = None
        self.parsed_table: str | None = None
        self.malformed_table: str | None = None
        self.hdfs_run_root: str | None = None

    # --------------------------------------------------
    # Command execution
    # --------------------------------------------------

    def _run_command(
        self,
        command: list[str],
        label: str,
    ) -> str:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="replace",
        )

        output = (result.stdout or "") + (result.stderr or "")

        if result.returncode != 0:
            raise RuntimeError(
                f"{label} failed (exit {result.returncode}):\n"
                f"{output[-5000:]}"
            )

        return result.stdout or ""

    def _run_beeline(
        self,
        command: list[str],
        label: str,
    ) -> str:
        """Run Beeline through a pseudo-terminal for JLine compatibility."""

        master_fd, slave_fd = pty.openpty()
        output = bytearray()

        env = dict(os.environ)
        env["TERM"] = "dumb"

        try:
            process = subprocess.Popen(
                command,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                env=env,
                close_fds=True,
            )
        except Exception:
            os.close(master_fd)
            raise
        finally:
            os.close(slave_fd)

        try:
            while True:
                try:
                    chunk = os.read(master_fd, 65536)

                    if not chunk:
                        break

                    output.extend(chunk)

                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
        finally:
            os.close(master_fd)

        return_code = process.wait()

        result = output.decode("utf-8", errors="replace")
        result = result.replace("\r\n", "\n").replace("\r", "\n")

        if return_code != 0:
            raise RuntimeError(
                f"{label} failed (exit {return_code}):\n"
                f"{result[-5000:]}"
            )

        return result

    def _beeline_base(self) -> list[str]:
        return [
            self.beeline,
            "-u",
            hive_url(),
            "--silent=true",
            "--showHeader=false",
            "--outputformat=tsv2",
        ]

    def _sql(
        self,
        statement: str,
        label: str = "Hive SQL",
    ) -> str:
        command = self._beeline_base()

        sql = (
            f"ADD JAR {SERDE_JAR};\n"
            f"{statement}"
        )

        command.extend(["-e", sql])

        return self._run_beeline(command, label)

    def _script(
        self,
        script_path: Path,
        hivevars: dict[str, str],
        label: str,
        add_serde: bool = False,
    ) -> str:
        if not script_path.is_file():
            raise FileNotFoundError(script_path)

        command = self._beeline_base()

        for key, value in hivevars.items():
            command.extend([
                "--hivevar",
                f"{key}={value}",
            ])

        script_sql = script_path.read_text(encoding="utf-8")

        if add_serde:
            script_sql = (
                f"ADD JAR {SERDE_JAR};\n"
                f"{script_sql}"
            )

        command.extend(["-e", script_sql])

        return self._run_beeline(command, label)
    def _hdfs(
        self,
        args: list[str],
        label: str = "HDFS command",
    ) -> str:
        return self._run_command(
            [self.hdfs, "dfs", *args],
            label,
        )

    def _hdfs_delete(self, path: str) -> None:
        self._hdfs(
            ["-rm", "-r", "-f", path],
            f"Delete HDFS path {path}",
        )

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _count(
        self,
        table: str,
        batch_id: int | None = None,
    ) -> int:
        where = ""

        if batch_id is not None:
            where = f" WHERE batch_id = {int(batch_id)}"

        output = self._sql(
            f"SELECT COUNT(*) FROM {table}{where}",
            f"Count rows in {table}",
        )

        lines = [
            line.strip()
            for line in output.splitlines()
            if line.strip()
        ]

        if not lines:
            raise RuntimeError(
                f"No count returned for {table}:\n{output[-3000:]}"
            )

        try:
            return int(lines[-1])
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid count for {table}: {lines[-1]!r}\n"
                f"{output[-3000:]}"
            ) from exc

    def _require_setup(self, run_id: str) -> None:
        if self.run_id != run_id:
            raise RuntimeError(
                "Hive setup must be called before this operation."
            )

        if not all((
            self.raw_table,
            self.parsed_table,
            self.malformed_table,
            self.hdfs_run_root,
        )):
            raise RuntimeError("Hive tables are not initialized.")

    def _local_run(self, run_id: str) -> Path:
        return runs_root() / run_id

    # --------------------------------------------------
    # Setup
    # --------------------------------------------------

    def setup(self, run_id: str) -> None:
        safe_id = safe_run_id(run_id)
        self.run_id = run_id

        self.raw_table = f"nasa_raw_{safe_id}"
        self.parsed_table = f"nasa_parsed_{safe_id}"
        self.malformed_table = f"nasa_malformed_{safe_id}"

        self.hdfs_run_root = f"{hive_root()}/{safe_id}"

        local_dir = self._local_run(run_id)

        if local_dir.exists():
            shutil.rmtree(local_dir)

        (local_dir / "logs").mkdir(
            parents=True,
            exist_ok=True,
        )

        # Standard Hive HDFS temporary directory permissions.
        # HDFS /tmp should be writable by all users.
        self._hdfs(
            ["-chmod", "1777", "/tmp"],
            "Set HDFS temporary directory permissions",
        )

        # Remove only tables belonging to this run.
        self._sql(
            f"""
            DROP TABLE IF EXISTS {self.raw_table};
            DROP TABLE IF EXISTS {self.parsed_table};
            DROP TABLE IF EXISTS {self.malformed_table};
            """,
            "Drop previous run tables",
        )

        # Remove only this run's storage directory.
        self._hdfs_delete(self.hdfs_run_root)

        raw_location = f"{self.hdfs_run_root}/raw"
        parsed_location = f"{self.hdfs_run_root}/parsed"
        malformed_location = f"{self.hdfs_run_root}/malformed"

        self._hdfs(
            [
                "-mkdir",
                "-p",
                raw_location,
                parsed_location,
                malformed_location,
            ],
            "Create Hive storage directories",
        )

        # The HiveServer2 user is anonymous in this environment.
        self._hdfs(
            ["-chmod", "-R", "777", self.hdfs_run_root],
            "Set run storage permissions",
        )

        ddl = f"""
        CREATE EXTERNAL TABLE {self.raw_table} (
            line STRING
        )
        PARTITIONED BY (
            batch_id INT
        )
        ROW FORMAT SERDE 'nasa.etl.Latin1LineSerDe'
        STORED AS TEXTFILE
        LOCATION '{raw_location}';

        CREATE EXTERNAL TABLE {self.parsed_table} (
            host STRING,
            log_timestamp STRING,
            log_date DATE,
            log_hour INT,
            http_method STRING,
            resource_path STRING,
            protocol_version STRING,
            status_code INT,
            bytes_transferred BIGINT
        )
        PARTITIONED BY (
            batch_id INT
        )
        STORED AS ORC
        LOCATION '{parsed_location}';

        CREATE EXTERNAL TABLE {self.malformed_table} (
            line STRING
        )
        PARTITIONED BY (
            batch_id INT
        )
        STORED AS ORC
        LOCATION '{malformed_location}';
        """

        self._sql(
            ddl,
            "Create run-specific Hive tables",
        )

    # --------------------------------------------------
    # Process a batch
    # --------------------------------------------------

    def process_batch(
        self,
        run_id: str,
        batch_id: int,
        batch_path: Path,
    ) -> BatchResult:
        self._require_setup(run_id)

        batch_id = int(batch_id)
        batch_path = Path(batch_path)

        if batch_id < 1:
            raise ValueError(f"Invalid batch ID: {batch_id}")

        if not batch_path.is_file():
            raise FileNotFoundError(batch_path)

        raw_partition = (
            f"{self.hdfs_run_root}/raw/batch_id={batch_id}"
        )

        self._hdfs(
            ["-mkdir", "-p", raw_partition],
            f"Create raw partition {batch_id}",
        )

        self._hdfs(
            ["-put", "-f", str(batch_path), raw_partition],
            f"Upload batch {batch_id}",
        )

        self._sql(
            f"""
            ALTER TABLE {self.raw_table}
            ADD IF NOT EXISTS PARTITION (batch_id={batch_id})
            LOCATION '{raw_partition}';
            """,
            f"Register raw partition {batch_id}",
        )

        self._script(
            SQL_DIR / "parse.hql",
            {
                "raw_table": self.raw_table,
                "parsed_table": self.parsed_table,
                "malformed_table": self.malformed_table,
                "batch_id": str(batch_id),
            },
            f"Parse batch {batch_id}",
            add_serde=True,
        )

        record_count = self._count(
            self.raw_table,
            batch_id,
        )

        parsed_count = self._count(
            self.parsed_table,
            batch_id,
        )

        malformed_count = self._count(
            self.malformed_table,
            batch_id,
        )

        if parsed_count + malformed_count != record_count:
            raise RuntimeError(
                f"Batch {batch_id} count mismatch: "
                f"records={record_count}, "
                f"parsed={parsed_count}, "
                f"malformed={malformed_count}"
            )

        return BatchResult(
            record_count=record_count,
            parsed_count=parsed_count,
            malformed_count=malformed_count,
        )

    # --------------------------------------------------
    # Read query output
    # --------------------------------------------------

        # --------------------------------------------------
    # Read query output
    # --------------------------------------------------

    def _read_hdfs_tsv(
        self,
        directory: str,
    ) -> list[tuple[str, ...]]:
        output = self._hdfs(
            ["-cat", f"{directory}/*"],
            f"Read query output {directory}",
        )

        rows = []

        for line in output.splitlines():
            if line.strip():
                rows.append(tuple(line.split("\t")))

        return rows
    # --------------------------------------------------
    # Run Q1, Q2 and Q3
    # --------------------------------------------------

    def run_queries(
        self,
        run_id: str,
        batch_id: int | None = None,
    ) -> QueryResults:
        self._require_setup(run_id)

        if batch_id is None:
            batch_filter = ""
            suffix = "all"
        else:
            batch_id = int(batch_id)

            if batch_id < 1:
                raise ValueError(f"Invalid batch ID: {batch_id}")

            batch_filter = f"WHERE batch_id = {batch_id}"
            suffix = f"batch_{batch_id}"

        output_dir = f"{self.hdfs_run_root}/out/{suffix}"

        self._hdfs_delete(output_dir)

        self._script(
            SQL_DIR / "queries.hql",
            {
                "parsed_table": self.parsed_table,
                "batch_filter": batch_filter,
                "output_dir": output_dir,
            },
            f"Run Hive queries ({suffix})",
        )

        q1_rows = self._read_hdfs_tsv(
            f"{output_dir}/q1"
        )

        q2_rows = self._read_hdfs_tsv(
            f"{output_dir}/q2"
        )

        q3_rows = self._read_hdfs_tsv(
            f"{output_dir}/q3"
        )

        q1 = []

        for row in q1_rows:
            if len(row) != 4:
                raise RuntimeError(f"Invalid Q1 row: {row!r}")

            q1.append((
                row[0],
                int(row[1]),
                int(row[2]),
                int(row[3]),
            ))

        q2 = []

        for row in q2_rows:
            if len(row) != 4:
                raise RuntimeError(f"Invalid Q2 row: {row!r}")

            q2.append((
                row[0],
                int(row[1]),
                int(row[2]),
                int(row[3]),
            ))

        q3 = []

        for row in q3_rows:
            if len(row) != 6:
                raise RuntimeError(f"Invalid Q3 row: {row!r}")

            q3.append((
                row[0],
                int(row[1]),
                int(row[2]),
                int(row[3]),
                float(row[4]),
                int(row[5]),
            ))

        return QueryResults(
            q1=q1,
            q2=q2,
            q3=q3,
        )

    # --------------------------------------------------
    # Cleanup
    # --------------------------------------------------

    def cleanup(self, run_id: str) -> None:
        if os.environ.get("ETL_HIVE_KEEP"):
            print(
                f"Hive cleanup skipped for {run_id}; "
                "ETL_HIVE_KEEP is set."
            )
            return

        safe_id = safe_run_id(run_id)

        raw_table = f"nasa_raw_{safe_id}"
        parsed_table = f"nasa_parsed_{safe_id}"
        malformed_table = f"nasa_malformed_{safe_id}"

        self._sql(
            f"""
            DROP TABLE IF EXISTS {raw_table};
            DROP TABLE IF EXISTS {parsed_table};
            DROP TABLE IF EXISTS {malformed_table};
            """,
            "Drop run-specific Hive tables",
        )

        self._hdfs_delete(
            f"{hive_root()}/{safe_id}"
        )

        local_dir = self._local_run(run_id)

        if local_dir.exists():
            shutil.rmtree(local_dir)

        print(f"Hive cleanup completed for {run_id}.")
