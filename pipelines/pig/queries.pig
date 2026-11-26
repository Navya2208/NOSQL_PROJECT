-- Q1, Q2 and Q3 (SPEC.md section 5). Owner: Aprajita.
--
-- Parameters: input (one batch's parsed folder, or a glob of every batch for the whole run),
--             out (output folder)
-- The whole-run answer is computed from every parsed record, never from batch results.
-- Results are written as JSON so any character in a path survives exactly.

recs = LOAD '$input' USING BinStorage() AS (host:chararray, timestamp:chararray, log_date:chararray,
    log_hour:int, http_method:chararray, resource_path:chararray, protocol_version:chararray,
    status_code:int, bytes_transferred:long);

-- Q1: daily traffic per (log_date, status_code)
by_day_status = GROUP recs BY (log_date, status_code);
q1 = FOREACH by_day_status GENERATE FLATTEN(group) AS (log_date, status_code),
    COUNT_STAR(recs) AS request_count, SUM(recs.bytes_transferred) AS total_bytes;
STORE q1 INTO '$out/q1' USING JsonStorage();

-- Q2: top 20 paths; distinct hosts with a nested DISTINCT; ties by path in character order
by_path = GROUP recs BY resource_path;
per_path = FOREACH by_path {
    hosts = DISTINCT recs.host;
    GENERATE group AS resource_path, COUNT_STAR(recs) AS request_count,
             SUM(recs.bytes_transferred) AS total_bytes, COUNT(hosts) AS distinct_host_count;
};
ranked = ORDER per_path BY request_count DESC, resource_path ASC;
q2 = LIMIT ranked 20;
STORE q2 INTO '$out/q2' USING JsonStorage();

-- Q3: hourly errors; nested FILTER keeps the 400-599 records, nested DISTINCT their hosts
flagged = FOREACH recs GENERATE log_date, log_hour, host,
    ((status_code >= 400 AND status_code <= 599) ? 1 : 0) AS is_error;
by_hour = GROUP flagged BY (log_date, log_hour);
q3 = FOREACH by_hour {
    errors = FILTER flagged BY is_error == 1;
    error_hosts = DISTINCT errors.host;
    GENERATE FLATTEN(group) AS (log_date, log_hour),
             SUM(flagged.is_error) AS error_request_count,
             COUNT_STAR(flagged) AS total_request_count,
             (double) SUM(flagged.is_error) / (double) COUNT_STAR(flagged) AS error_rate,
             COUNT(error_hosts) AS distinct_error_hosts;
};
STORE q3 INTO '$out/q3' USING JsonStorage();
