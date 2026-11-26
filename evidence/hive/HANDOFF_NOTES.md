# Hive Handoff for Navya

## Implementation
The package contains the Hive pipeline, SQL scripts, custom Latin1 line SerDe source and compiled JAR.

## Requested parsing fixes
- Request parsing RLIKE and regexp_extract patterns use the `(?d)` prefix.
- The request-trimming regex uses `(?d)^[ ]+|[ ]+$`.
- Both CR-removal expressions use `(?d)\\r$`.
- The Hive pipeline sets `textinputformat.record.delimiter` to a newline for the session and raw TEXTFILE table.

## Validation evidence
- Golden sample: `20261126-103523-reference` vs `20261126-135336-hive` — IDENTICAL (11 checks).
- Full dataset: `20261126-144501-reference` vs `20261126-152137-hive` — IDENTICAL (11 checks).
- Full dataset batch size: 500,000; 8 batches; 3,461,613 total records; 3,461,612 parsed; 1 malformed.
- Automated tests: 33 passed. Latest recorded run: 1.40 seconds.
- Python syntax check passed.

## Included evidence
- `hive_golden_sample_comparison.png`
- `hive_full_dataset_comparison.png`

## Optional shared-regex suggestion
The optional suggestion to use one shared line regex was not implemented; the existing field-specific parsing expressions are retained.
