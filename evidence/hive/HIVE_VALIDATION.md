# Hive Pipeline Validation

## Golden Sample Comparison

Reference run: 20261126-103523-reference
Hive run: 20261126-135336-hive

Result: IDENTICAL

All 11 comparison checks passed.

## Full Dataset Comparison

Reference run: 20261126-144501-reference
Hive run: 20261126-152137-hive

Batch size: 500000
Batches: 8
Total records: 3,461,613
Parsed records: 3,461,612
Malformed records: 1

Result: IDENTICAL

All 11 comparison checks passed.

## Automated Tests

Command:

.venv/bin/python -m pytest

Result:

33 passed in 1.40 seconds

Test breakdown:
- test_batching.py: 6 passed
- test_end_to_end.py: 10 passed
- test_menu.py: 8 passed
- test_spec_parsing.py: 9 passed

## Build Verification

- Latin1LineSerDe.java: Present
- latin1-serde.jar: Present
- Python syntax check: Passed
