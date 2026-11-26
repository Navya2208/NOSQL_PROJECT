import os
import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).parent))

TEST_DSN = os.environ.get("ETL_TEST_PG_DSN", "postgresql://etl@localhost:5433/etl_test")
ADMIN_DSN = TEST_DSN.rsplit("/", 1)[0] + "/postgres"

# Example lines from SPEC.md section 2.4, plus two error lines for Q3. 11 lines: 7 parsed, 4 malformed.
SAMPLE_JUL = (
    b'host-a.example.com - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245\n'
    b'host-b.example.com - - [01/Jul/1995:13:05:12 -0400] "GET /images/logo.gif HTTP/1.0" 304 -\n'
    b'host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html" 200 1024\n'
    b'host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /some file.html HTTP/1.0" 200 512\n'
    b'host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "" 400 0\n'
    b'host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html HTTP/1.0" 200\n'
    b'alyssa.p'
)  # ends with a cut-off line and no newline, like the real July file
SAMPLE_AUG = (
    b'host-e.example.com - - [02/Jul/1995 08:15:00 -0400] "GET / HTTP/1.0" 200 100\n'
    b'\n'
    b'host-a.example.com - - [04/Aug/1995:10:00:00 -0400] "GET /index.html HTTP/1.0" 404 0\r\n'
    b'host-b.example.com - - [04/Aug/1995:10:30:00 -0400] "GET /index.html HTTP/1.0" 200 300'
)  # no newline at the end of the last file


@pytest.fixture
def sample_files(tmp_path):
    jul, aug = tmp_path / "jul.log", tmp_path / "aug.log"
    jul.write_bytes(SAMPLE_JUL)
    aug.write_bytes(SAMPLE_AUG)
    return [jul, aug]


@pytest.fixture
def pg_dsn():
    """A clean etl_test database with the schema. Skips if PostgreSQL is not reachable."""
    try:
        with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
            if not conn.execute("SELECT 1 FROM pg_database WHERE datname = 'etl_test'").fetchone():
                conn.execute("CREATE DATABASE etl_test")
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable at {ADMIN_DSN}: {exc}")
    from etl import db
    db.init_schema(TEST_DSN, reset=True)
    return TEST_DSN
