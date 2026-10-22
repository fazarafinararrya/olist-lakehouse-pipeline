# Olist Lakehouse Pipeline

Batch data pipeline for the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(~100k orders, 9 tables): **CSV -> raw -> silver** on PySpark with an S3-compatible lake, data quality
gates, Prefect orchestration, Terraform-managed AWS infrastructure and tests. Gold (star schema with
dbt on Athena) is next.

The focus is on what makes a pipeline trustworthy: explicit contracts, idempotent reruns,
reconciliation and data quality, documented decisions, and an operations runbook.

## Architecture

```
Olist CSV --Spark (header contract)--> raw (Parquet, partitioned by batch)
          --clean/type/dedup/flag---> silver (Parquet)
          --data quality gate-------> pass | fail + JSON report
Prefect orchestrates; Terraform provisions S3, Glue DBs, Athena workgroup, IAM and a cost budget.
```

Details and design decisions: [`docs/architecture.md`](docs/architecture.md).
What is wrong with the data and how each issue is handled: [`docs/data_profile.md`](docs/data_profile.md).
When it breaks: [`docs/runbook.md`](docs/runbook.md) and the RCA template in `docs/incidents/`.

## Stack

| concern | tool |
|---|---|
| processing | PySpark 3.5 / Spark SQL |
| lake storage | S3 (AWS) or MinIO (local), Parquet |
| orchestration | Prefect 3 (retries, rerun, run history) |
| data quality | own checks with error/warn severity + JSON report |
| infrastructure | Terraform (S3, Glue Data Catalog, Athena, IAM, Budgets) |
| runtime | Docker Compose |
| tests / CI | pytest on local Spark, ruff, GitHub Actions (incl. `terraform validate`) |

## Quickstart

The Olist CSVs are not in the repo. Put the nine files in `data/source/` using the original names
(`olist_orders_dataset.csv`, ..., `product_category_name_translation.csv`).

### 1. Local disk (no Docker, no cloud)

Needs Python 3.10+ and Java 17+.

```bash
pip install -e ".[dev]"
make test         # unit + end-to-end tests on local Spark
make run-local    # raw -> silver -> quality, writes to data/lake and data/reports
```

Compare the row counts in the log with the table in `docs/data_profile.md`.

### 2. MinIO + Prefect UI (Docker)

```bash
make up           # MinIO (console :9001), Prefect (UI :4200)
make run          # runs the Prefect flow against s3a://olist-* on MinIO
```

### 3. AWS

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # set your email
make tf-init && make tf-plan && make tf-apply
# create an access key for the `olist-pipeline` IAM user, store it in ~/.aws/credentials,
# then set LAKE_* and AWS_REGION from `terraform output` (see .env.example)
python flows/olist_flow.py
make tf-destroy   # when you are done practising
```

A monthly budget with email alerts is part of the Terraform. Nothing in this project needs EMR,
Redshift, NAT gateways or always-on compute.

## Reruns and batches

`--batch-id` (default: today, UTC) identifies a load. Raw is partitioned by `ingest_batch_id` with dynamic
partition overwrite, so rerunning a batch replaces only that batch:

```bash
python flows/olist_flow.py --batch-id 2026-10-22
python -m olist_pipeline.cli silver --batch-id 2026-10-22
```

## Tests

`pytest` runs on a local Spark session: transformation rules on tiny in-memory data, the CSV reader
(multi-line quoted fields, BOM, schema drift), every data quality check, and an end-to-end run that
executes twice to prove idempotency.

## Status

| phase | state |
|---|---|
| Terraform, Docker Compose, CI | written, needs first real `terraform apply` / `docker compose up` |
| raw -> silver, data quality, Prefect flow | written, needs first real run against the full dataset |
| Glue Catalog registration + Athena queries | next |
| gold star schema (dbt-athena) | planned |
| Airbyte ingestion, monitoring/alerts, failure injection + RCAs | planned |
| dashboard (Tableau Public) | planned |

Roadmap detail in `docs/architecture.md`.
