-- Parse job for one batch (SPEC.md section 2). Owner: Aprajita.
--
-- Parameters: loader_jar, input (batch file), parsed (output folder), counts (output folder)
-- All parsing, malformed detection and counting happen here, in Pig Latin.
--
-- The regexes are the SPEC.md 2.1 and 2.3 patterns with Java's (?d) flag in front.
-- (?d) only makes "\n" the sole line terminator, so "." also matches characters such as
-- U+0085 (decoded from byte 0x85), exactly as in Python and MongoDB. Pig string literals
-- need every regex backslash doubled.

REGISTER '$loader_jar';

raw = LOAD '$input' USING nasa.etl.Latin1TextLoader() AS (line:chararray);

-- SPEC.md section 1: remove one trailing carriage return.
lines = FOREACH raw GENERATE REPLACE(line, '(?d)\\r$', '') AS line;

matched = FOREACH lines GENERATE
    REGEX_EXTRACT_ALL(line, '(?d)^(\\S+) \\S+ \\S+ \\[((0[1-9]|[12]\\d|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\\d{4}):([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d [+-]\\d{4})\\] "(.*)" (\\d{3}) (\\d+|-)$') AS m;

-- A line that does not match is malformed: counted below, never parsed.
SPLIT matched INTO good IF m IS NOT NULL, malformed OTHERWISE;

fields = FOREACH good GENERATE FLATTEN(m) AS (host:chararray, ts:chararray, day:chararray,
    mon:chararray, year:chararray, hour:chararray, request:chararray, status:chararray, nbytes:chararray);

-- SPEC.md 2.3: trim spaces (only spaces) from both ends, then split the request.
trimmed = FOREACH fields GENERATE host, ts, day, mon, year, hour, status, nbytes,
    REPLACE(request, '(?d)^ +| +$', '') AS request;

split_req = FOREACH trimmed GENERATE host, ts, day, mon, year, hour, status, nbytes, request,
    REGEX_EXTRACT_ALL(request, '(?d)^(\\S+)\\s+(.+?)(?:\\s+(HTTP/\\S+))?$') AS r,
    INDEXOF('JanFebMarAprMayJunJulAugSepOctNovDec', mon, 0) / 3 AS mi;

parsed = FOREACH split_req GENERATE
    host,
    ts AS timestamp,
    CONCAT(year, '-', SUBSTRING('010203040506070809101112', 2 * mi, 2 * mi + 2), '-', day) AS log_date,
    (int) hour AS log_hour,
    (r IS NULL ? request : (chararray) r.$0) AS http_method,
    (r IS NULL ? '' : (chararray) r.$1) AS resource_path,
    (r IS NULL OR r.$2 IS NULL ? '' : (chararray) r.$2) AS protocol_version,
    (int) status AS status_code,
    (nbytes == '-' ? 0L : (long) nbytes) AS bytes_transferred;

-- Pig's binary format keeps every string exactly, including tabs and control bytes.
STORE parsed INTO '$parsed' USING BinStorage();

-- Record, parsed and malformed counts for the controller, counted by Pig.
everything = GROUP matched ALL;
counts = FOREACH everything {
    ok = FILTER matched BY m IS NOT NULL;
    GENERATE COUNT_STAR(matched) AS records, COUNT_STAR(ok) AS parsed,
             COUNT_STAR(matched) - COUNT_STAR(ok) AS malformed;
};
STORE counts INTO '$counts' USING JsonStorage();
