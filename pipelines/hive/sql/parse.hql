
-- ============================================================
-- Parse valid records into the run-specific parsed table
--
-- Parameters:
--   raw_table    : run-specific raw table
--   parsed_table : run-specific parsed table
--   malformed_table : run-specific malformed table
--   batch_id     : current batch ID
-- ============================================================

INSERT OVERWRITE TABLE ${hivevar:parsed_table}
PARTITION (batch_id=${hivevar:batch_id})
SELECT
    host,
    log_timestamp,
    log_date,
    log_hour,
    http_method,
    resource_path,
    protocol_version,
    status_code,
    bytes_transferred
FROM
(
    SELECT
        host,
        log_timestamp,
        CAST(
            concat(
                year, '-',
                CASE month
                    WHEN 'Jan' THEN '01'
                    WHEN 'Feb' THEN '02'
                    WHEN 'Mar' THEN '03'
                    WHEN 'Apr' THEN '04'
                    WHEN 'May' THEN '05'
                    WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07'
                    WHEN 'Aug' THEN '08'
                    WHEN 'Sep' THEN '09'
                    WHEN 'Oct' THEN '10'
                    WHEN 'Nov' THEN '11'
                    WHEN 'Dec' THEN '12'
                END,
                '-', day
            ) AS DATE
        ) AS log_date,
        log_hour,

        CASE
            WHEN trimmed_request RLIKE '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$'
            THEN regexp_extract(
                trimmed_request,
                '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$',
                1
            )
            ELSE trimmed_request
        END AS http_method,

        CASE
            WHEN trimmed_request RLIKE '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$'
            THEN regexp_extract(
                trimmed_request,
                '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$',
                2
            )
            ELSE ''
        END AS resource_path,

        CASE
            WHEN trimmed_request RLIKE '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$'
            THEN regexp_extract(
                trimmed_request,
                '^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$',
                3
            )
            ELSE ''
        END AS protocol_version,

        status_code,

        CASE
            WHEN bytes_text = '-' THEN CAST(0 AS BIGINT)
            ELSE CAST(bytes_text AS BIGINT)
        END AS bytes_transferred

    FROM
    (
        SELECT
            host,
            log_timestamp,
            day,
            month,
            year,
            log_hour,
            regexp_replace(request, '^[ ]+|[ ]+$', '') AS trimmed_request,
            status_code,
            bytes_text
        FROM
        (
            SELECT
                line,
                regexp_extract(line, '(?d)^(\\S+) \\S+ \\S+ \\[', 1) AS host,
                regexp_extract(line, '(?d)^\\S+ \\S+ \\S+ \\[([^]]+)\\]', 1) AS log_timestamp,
                regexp_extract(line, '(?d)^\\S+ \\S+ \\S+ \\[(\\d{2})/', 1) AS day,
                regexp_extract(line, '(?d)^\\S+ \\S+ \\S+ \\[\\d{2}/([A-Za-z]{3})/', 1) AS month,
                regexp_extract(line, '(?d)^\\S+ \\S+ \\S+ \\[\\d{2}/[A-Za-z]{3}/(\\d{4}):', 1) AS year,

                CAST(
                    regexp_extract(line, '(?d)^\\S+ \\S+ \\S+ \\[\\d{2}/[A-Za-z]{3}/\\d{4}:(\\d{2}):', 1)
                    AS INT
                ) AS log_hour,

                regexp_extract(line, '(?d)\\] "(.*)" (\\d{3}) (\\d+|-)$', 1) AS request,

                CAST(
                    regexp_extract(line, '(?d)\\] ".*" (\\d{3}) (\\d+|-)$', 1)
                    AS INT
                ) AS status_code,

                regexp_extract(line, '(?d)\\] ".*" \\d{3} (\\d+|-)$', 1) AS bytes_text

            FROM
            (
                SELECT regexp_replace(line, '\\r$', '') AS line
                FROM ${hivevar:raw_table}
                WHERE batch_id = ${hivevar:batch_id}
            ) clean

            WHERE line RLIKE '(?d)^(\\S+) \\S+ \\S+ \\[((0[1-9]|[12]\\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\\d{4}):([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d [+-]\\d{4})\\] "(.*)" (\\d{3}) (\\d+|-)$'
        ) extracted
    ) valid_requests
) final_rows;


-- ============================================================
-- Write malformed records into the run-specific malformed table
-- ============================================================

INSERT OVERWRITE TABLE ${hivevar:malformed_table}
PARTITION (batch_id=${hivevar:batch_id})
SELECT line
FROM
(
    SELECT
        line,
        line RLIKE '(?d)^(\\S+) \\S+ \\S+ \\[((0[1-9]|[12]\\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\\d{4}):([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d [+-]\\d{4})\\] "(.*)" (\\d{3}) (\\d+|-)$' AS is_valid
    FROM
    (
        SELECT regexp_replace(line, '\\r$', '') AS line
        FROM ${hivevar:raw_table}
        WHERE batch_id = ${hivevar:batch_id}
    ) clean
) checked
WHERE is_valid = FALSE;

