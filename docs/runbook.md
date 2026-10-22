# Runbook

First stop when the `olist-raw-to-silver` flow fails. Each entry: symptom, likely cause,
how to confirm, fix. Add a new entry (and an RCA under `docs/incidents/`) after every real incident.

## Quick facts

- Flow: `flows/olist_flow.py` (Prefect). Stages: `ingest-table` x9 -> `build-silver` -> `data-quality`.
- A *batch* is identified by `--batch-id` (default: today, UTC). Raw is partitioned by batch.
- **Reruns are safe**: raw replaces only that batch's partition; silver is rebuilt from that batch.
- Data quality report per batch: `data/reports/dq_<batch_id>.json`.

## Rerun / recovery

| situation | command |
|---|---|
| rerun the whole batch | `python flows/olist_flow.py --batch-id 2026-10-22` |
| only one stage | `python -m olist_pipeline.cli silver --batch-id 2026-10-22` |
| inspect DQ failures without failing | `python -m olist_pipeline.cli quality --batch-id ... --no-fail` |

## Failure catalogue

### F1 - `SchemaDriftError: missing expected columns [...]` (stage `ingest-table`)
- **Cause**: the upstream CSV renamed or dropped a column.
- **Confirm**: compare the file header with `src/olist_pipeline/schemas.py`.
- **Fix**: if the change is intentional, update `SOURCE_COLUMNS` (and silver) in one reviewed change, then rerun.
  Do **not** loosen the check. Extra columns are tolerated and logged as warnings.

### F2 - `FileNotFoundError: Source file for '<table>' not found`
- **Cause**: file missing or late.
- **Fix**: place the file in `SOURCE_DIR`, rerun the batch. Prefect retries 2x (30 s) before failing.

### F3 - `DataQualityError: N data quality check(s) failed` (stage `data-quality`)
- **Confirm**: open `data/reports/dq_<batch>.json`, filter `severity=error` and `passed=false`.
- `unique(...)` failed -> duplicates entered upstream or a join fan-out was introduced in silver.
- `fk(...)` failed -> a child table is ahead of its parent (late file) or a key was altered.
- `accepted_values(order_status)` failed -> new status upstream; decide if it is real, then extend `ORDER_STATUSES`.
- **Fix**: correct the cause, rerun the batch. Never rerun with `--no-fail` to "get green".

### F4 - Spark on `s3a://`: `ClassNotFoundException` / `NoSuchMethodError` / `403`
- **Cause (classes)**: `hadoop-aws` and `aws-java-sdk-bundle` versions do not match Spark's Hadoop (3.3.4 for Spark 3.5.x). See `spark.py`.
- **Cause (403)**: wrong credentials/profile, wrong region, or the IAM policy lacks the bucket.
- **Confirm**: `aws sts get-caller-identity`, `aws s3 ls s3://<bucket>`.
- **Fix**: align versions; fix credentials; for MinIO make sure `S3_ENDPOINT_URL` is set (path-style access).

### F5 - Spark out of memory / job very slow
- **Likely hotspot**: `geolocation` (1M rows) and `reviews` (`multiLine` => one task per file).
- **Confirm**: Spark UI (http://localhost:4040 while running): long tasks, spill, skewed stage.
- **Fix**: raise driver memory, raise `SPARK_SHUFFLE_PARTITIONS`, avoid joining customers/sellers to raw geolocation (use `geolocation_zip`).

## Escalation

Record: batch id, failing stage, error text, DQ report, what was tried. Open an RCA from
`docs/incidents/TEMPLATE-RCA.md` for any failure that took more than one rerun to fix.
