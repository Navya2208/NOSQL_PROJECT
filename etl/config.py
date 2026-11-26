"""Settings, read from environment variables at call time so tests can override them."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PIPELINE_NAMES = ("mongodb", "mapreduce", "pig", "hive")

INPUT_FILE_NAMES = ("NASA_access_log_Jul95", "NASA_access_log_Aug95")  # read in this order


def pg_dsn() -> str:
    return os.environ.get("ETL_PG_DSN", "postgresql://etl@localhost:5433/etl")


def mongo_uri() -> str:
    return os.environ.get("ETL_MONGO_URI", "mongodb://localhost:27017")


def data_dir() -> Path:
    return Path(os.environ.get("ETL_DATA_DIR", ROOT / "data"))


def work_dir() -> Path:
    return Path(os.environ.get("ETL_WORK_DIR", ROOT / "work"))


def default_inputs() -> list[Path]:
    return [data_dir() / name for name in INPUT_FILE_NAMES]
