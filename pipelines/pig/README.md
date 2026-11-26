# Pig pipeline (owner: Aprajita)

All parsing, malformed detection, counting and query work runs in **Pig Latin**. Python only starts Pig and reads the results back; it does no parsing, arithmetic or sorting.

## How it works

```
controller --batch k--> process_batch --> parse.pig  (Pig, local mode)
   Latin1TextLoader          one chararray per line, bytes decoded as Latin-1
   REPLACE                   remove one trailing \r
   REGEX_EXTRACT_ALL         SPEC.md 2.1 line regex
   SPLIT                     parsed / malformed
   REGEX_EXTRACT_ALL         SPEC.md 2.3 request regex -> method, path, protocol
   FOREACH ... GENERATE      log_date, log_hour, status, bytes ('-' -> 0)
   STORE BinStorage          parsed/batch_k     (binary, every string kept exactly)
   GROUP ALL + COUNT_STAR    counts/batch_k     records, parsed, malformed

run_queries(run, k)    queries.pig over parsed/batch_k
run_queries(run, None) queries.pig over parsed/batch_*   (whole run, never summed from batches)
   Q1  GROUP BY (log_date, status_code); COUNT_STAR, SUM
   Q2  GROUP BY resource_path; nested DISTINCT host; ORDER BY count DESC, path ASC; LIMIT 20
   Q3  GROUP BY (log_date, log_hour); nested FILTER 400-599; nested DISTINCT error hosts
   STORE JsonStorage    out/<scope>/q1, q2, q3
```

## Files

| File | Role |
|---|---|
| `pipeline.py` | `PigPipeline`: `setup`, `process_batch`, `run_queries`, `cleanup`; starts Pig and reads JSON results |
| `parse.pig` | Parse job for one batch, plus the record counts |
| `queries.pig` | Q1, Q2 and Q3 for one batch or the whole run |
| `java/nasa/etl/Latin1TextLoader.java` | Pig load function: one line per record, decoded as Latin-1. No parsing. Compiled automatically into `work/pig_udf/latin1-loader.jar` |
| `test_pig_pipeline.py` | Compares the pipeline with the reference on the sample plus 27 edge-case lines, at batch sizes 12 and 1000 |

## Design decisions

- **Latin-1 loader.** Pig's `TextLoader` decodes text as UTF-8, which would change the 18 NASA lines with non-ASCII bytes. The small Java loader only decodes bytes as Latin-1 (`SPEC.md` section 1) and splits on `\n` only, so `parse.pig` removes the trailing `\r` itself.
- **`(?d)` on every regex.** In Java, `.` does not match U+0085 (byte `0x85` decoded) unless `UNIX_LINES` mode is on. `(?d)` turns it on, so the `SPEC.md` regexes behave exactly as in Python and MongoDB. Pig string literals double every backslash.
- **`BinStorage` for parsed records.** Some real requests contain tabs and control bytes such as `\x01`, which would break any text delimiter. Pig's binary format stores every string exactly.
- **`JsonStorage` for results.** JSON escapes every character in a path, and the error rate round-trips as the identical 64-bit float.
- **Nested `DISTINCT` and `FILTER`.** Distinct hosts and error hosts are computed inside each group, the Pig idiom for per-group distinct counts.
- **Ties in Q2.** `ORDER BY request_count DESC, resource_path ASC`: Pig compares strings by character code, the same order as PostgreSQL's `COLLATE "C"`.
- **Missing libraries in Pig 0.18.0.** Pig ships four Apache Commons jars but leaves them off its own classpath: Collections 3 (needed by the reduce phase; Hadoop 3.4 no longer includes it), and Lang 3, Compress and Text (needed when no Hadoop install is found and Pig uses its bundled Hadoop). `pipeline.py` adds Pig's own copies to `PIG_CLASSPATH`. Pig appends these after Hadoop's jars, so a local Hadoop's newer versions still win. Tested both with Hadoop 3.4.3 and with no Hadoop at all.
- **Works with or without Hadoop.** With `HADOOP_HOME` set (`source scripts/mapreduce_env.sh`), Pig uses that Hadoop. Without it, Pig uses its bundled Hadoop 3.3.6. Both give identical results.

## Setup and run

```bash
scripts/dev_pig.sh install
source scripts/mapreduce_env.sh
.venv/bin/etl run --pipeline pig --input tests/golden/sample.log --batch-size 50
.venv/bin/etl run --pipeline pig --batch-size 500000
```

`dev_pig.sh` downloads Apache Pig 0.18.0 (the Hadoop 3 build, about 492 MB) into `work/pig` and checks Apache's published checksum. Pig runs in local mode on local files. Set `ETL_PIG_KEEP=1` to keep a run's working files in `work/pig_runs` for inspection.

## Tests

```bash
.venv/bin/pytest pipelines/pig -v
```

Takes about 2 minutes, because every Pig step starts a Java process.

## Results

| Run | Records | Batches | Malformed | Runtime | Compare with reference |
|---|---|---|---|---|---|
| Edge-case test, batch sizes 12 and 1000 | 38 | 5 / 3 | 12 | about 100 s for both | identical |
| Golden sample, batch size 50 | 346 | 7 | 1 | 83 s | IDENTICAL |
| Full data, batch size 500,000 | 3,461,613 | 8 | 1 | 262 s | IDENTICAL (also identical to MongoDB and MapReduce) |
