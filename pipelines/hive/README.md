# Hive pipeline — Saravanan

This is the Hive-specific implementation folder. In the merged project repository, keep it at `pipelines/hive/`.

## Files

| File | Purpose |
|---|---|
| `pipeline.py` | Python integration for the Hive pipeline |
| `sql/parse.hql` | HiveQL parsing/table setup |
| `sql/queries.hql` | Q1, Q2, and Q3 aggregations |
| `java/nasa/etl/Latin1LineSerDe.java` | Custom SerDe for ISO-8859-1 (Latin-1) input decoding |
| `build/latin1-serde.jar` | Compiled SerDe JAR used at runtime |

## Implementation notes

- Engine: Apache Hive 4.2.1.
- Input decoding: custom Latin-1 SerDe to preserve the agreed specification's ISO-8859-1 behavior.
- Batch handling: each input batch is loaded separately; July and August source files remain separate.
- Query scopes: batch-level queries and independently recomputed whole-run queries.
- Beeline execution: SQL is invoked with `beeline -e` in the verified implementation.
- Output reading: query output files are read from the Hive output directory without assuming a `part-*` filename.

## Verified full-data run

See the handoff-level `results/` directory for the run summary, representative query outputs, and SHA-256 verification. Run ID: `20261125-173521-hive`.

## Environment used for the successful run

- WSL Ubuntu
- Java 21
- Hadoop 3.4.3
- Hive 4.2.1
- Beeline at `jdbc:hive2://127.0.0.1:10000/nasa_logs`
- Project Python virtual environment with the `etl` CLI installed

This describes the environment used for the recorded run; it is not a claim that Navya must rerun Hive during MongoDB integration.
