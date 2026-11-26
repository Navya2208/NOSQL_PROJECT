# Hive Run Summary

## Run status

| Field | Value |
|---|---|
| Run ID | `20261125-173521-hive` |
| Pipeline | Hive |
| Status | Succeeded |
| Batch size | 500,000 raw lines |
| Number of batches | 8 |
| Raw records | 3,461,613 |
| Parsed records | 3,461,612 |
| Malformed records | 1 |
| Runtime | 2,608.6725 seconds (about 43 min 29 sec) |

## Batch distribution

| Batch | Source file | Raw line range | Lines |
|---:|---|---:|---:|
| 1 | NASA_access_log_Jul95 | 1–500,000 | 500,000 |
| 2 | NASA_access_log_Jul95 | 500,001–1,000,000 | 500,000 |
| 3 | NASA_access_log_Jul95 | 1,000,001–1,500,000 | 500,000 |
| 4 | NASA_access_log_Jul95 | 1,500,001–1,891,715 | 391,715 |
| 5 | NASA_access_log_Aug95 | 1–500,000 | 500,000 |
| 6 | NASA_access_log_Aug95 | 500,001–1,000,000 | 500,000 |
| 7 | NASA_access_log_Aug95 | 1,000,001–1,500,000 | 500,000 |
| 8 | NASA_access_log_Aug95 | 1,500,001–1,569,898 | 69,898 |

## Whole-run query output dimensions

| Query | Output rows | Meaning |
|---|---:|---|
| Q1 | 295 | Daily traffic grouped by date and status code |
| Q2 | 20 | Top 20 requested resource paths |
| Q3 | 1,369 | Hourly error analysis grouped by date and hour |

## Example whole-run Q2 rows

| Resource path | Request count | Total bytes | Distinct hosts |
|---|---:|---:|---:|
| `/images/NASA-logosmall.gif` | 208,723 | 131,725,740 | 81,510 |
| `/images/KSC-logosmall.gif` | 164,976 | 169,364,272 | 85,161 |
| `/images/MOSAIC-logosmall.gif` | 127,916 | — | — |

The dashes mean those remaining values were not retained in the available run summary; they are intentionally not guessed. Full Q2 output is available from the Hive report in the working environment.

## Commands used for evidence

```bash
etl runs
etl report --run 20261125-173521-hive --rows 25
etl verify --run 20261125-173521-hive
```

The successful run and SHA-256 verification are documented separately in this handoff.
