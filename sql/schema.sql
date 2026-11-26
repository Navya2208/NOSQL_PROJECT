-- Reporting database for the multi-pipeline ETL tool (SPEC.md section 6).
-- Safe to run more than once: every statement uses IF NOT EXISTS.
-- `etl init-db --reset` drops the tables first (all stored runs are lost).
--
-- batch_id on result rows:
--   1..n  results of Q1-Q3 over only that batch's records
--   0     results over every parsed record of the run (the final answer)

CREATE TABLE IF NOT EXISTS runs (
    run_id             TEXT PRIMARY KEY,          -- e.g. 20261119-143005-mongodb
    pipeline           TEXT NOT NULL,             -- mongodb, mapreduce, pig or hive (checked by the CLI)
    batch_size         INTEGER NOT NULL CHECK (batch_size > 0),
    input_files        TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ,
    runtime_seconds    DOUBLE PRECISION,
    total_records      BIGINT,
    parsed_records     BIGINT,
    malformed_records  BIGINT,
    batch_count        INTEGER,
    avg_batch_size     DOUBLE PRECISION,
    error_message      TEXT
);

CREATE TABLE IF NOT EXISTS batches (
    run_id           TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id         INTEGER NOT NULL CHECK (batch_id >= 1),
    record_count     INTEGER NOT NULL,
    parsed_count     INTEGER NOT NULL,
    malformed_count  INTEGER NOT NULL,
    source_file      TEXT NOT NULL,               -- raw input file the batch was cut from
    first_line       BIGINT NOT NULL,             -- 1-based line numbers within source_file
    last_line        BIGINT NOT NULL,
    sha256           TEXT NOT NULL,               -- of the exact batch bytes; checked by etl verify
    PRIMARY KEY (run_id, batch_id),
    CHECK (parsed_count + malformed_count = record_count),
    CHECK (last_line - first_line + 1 = record_count)
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    run_id         TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline       TEXT NOT NULL,
    batch_id       INTEGER NOT NULL CHECK (batch_id >= 0),
    executed_at    TIMESTAMPTZ NOT NULL,
    log_date       DATE NOT NULL,
    status_code    SMALLINT NOT NULL,
    request_count  BIGINT NOT NULL,
    total_bytes    BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, log_date, status_code)
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    run_id               TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline             TEXT NOT NULL,
    batch_id             INTEGER NOT NULL CHECK (batch_id >= 0),
    executed_at          TIMESTAMPTZ NOT NULL,
    resource_path        TEXT COLLATE "C" NOT NULL,
    request_count        BIGINT NOT NULL,
    total_bytes          BIGINT NOT NULL,
    distinct_host_count  BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, resource_path)
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    run_id                TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline              TEXT NOT NULL,
    batch_id              INTEGER NOT NULL CHECK (batch_id >= 0),
    executed_at           TIMESTAMPTZ NOT NULL,
    log_date              DATE NOT NULL,
    log_hour              SMALLINT NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count   BIGINT NOT NULL,
    total_request_count   BIGINT NOT NULL,
    error_rate            DOUBLE PRECISION NOT NULL,
    distinct_error_hosts  BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, log_date, log_hour)
);
