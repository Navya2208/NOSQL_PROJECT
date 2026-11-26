"""PostgreSQL connection and schema setup."""
from __future__ import annotations

import psycopg

from etl import config

SCHEMA_FILE = config.ROOT / "sql" / "schema.sql"
TABLES = ("q1_daily_traffic", "q2_top_resources", "q3_hourly_errors", "batches", "runs")


def connect(dsn: str | None = None) -> psycopg.Connection:
    """Autocommit connection. Multi-statement writes use conn.transaction() explicitly."""
    return psycopg.connect(dsn or config.pg_dsn(), autocommit=True)


def init_schema(dsn: str | None = None, reset: bool = False) -> None:
    """Creates the tables. With reset=True, drops them first, deleting every stored run."""
    with connect(dsn) as conn:
        with conn.transaction():
            if reset:
                conn.execute("DROP TABLE IF EXISTS " + ", ".join(TABLES) + " CASCADE")
            conn.execute(SCHEMA_FILE.read_text())
