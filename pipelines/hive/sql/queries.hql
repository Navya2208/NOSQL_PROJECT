
-- ============================================================
-- Q1: Request count and total bytes by date and status code
-- ============================================================

INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    CAST(log_date AS STRING),
    status_code,
    COUNT(*) AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM ${hivevar:parsed_table}
${hivevar:batch_filter}
GROUP BY log_date, status_code
ORDER BY log_date, status_code;


-- ============================================================
-- Q2: Top 20 resource paths by request count
-- ============================================================

INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    resource_path,
    COUNT(*) AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    COUNT(DISTINCT host) AS distinct_host_count
FROM ${hivevar:parsed_table}
${hivevar:batch_filter}
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
LIMIT 20;


-- ============================================================
-- Q3: Error statistics by date and hour
-- ============================================================

INSERT OVERWRITE DIRECTORY '${hivevar:output_dir}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    CAST(log_date AS STRING),
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        AS error_request_count,
    COUNT(*) AS total_request_count,
    CAST(
        SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        AS DOUBLE
    ) / COUNT(*) AS error_rate,
    COUNT(
        DISTINCT CASE
            WHEN status_code BETWEEN 400 AND 599 THEN host
            ELSE NULL
        END
    ) AS distinct_error_hosts
FROM ${hivevar:parsed_table}
${hivevar:batch_filter}
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
