"""Prefect flow: raw ingest (per table) -> silver -> data quality.

Run locally:   python flows/olist_flow.py
Rerun a batch: python flows/olist_flow.py --batch-id 2026-10-22   (idempotent)
"""

from __future__ import annotations

import argparse

from prefect import flow, get_run_logger, task

from olist_pipeline import pipeline, raw
from olist_pipeline.config import SOURCE_FILES, Settings
from olist_pipeline.spark import get_spark


@task(name="ingest-table", retries=2, retry_delay_seconds=30)
def ingest_table(table: str, batch_id: str) -> int:
    result = raw.ingest_table(get_spark(), Settings.from_env(), table, batch_id)
    get_run_logger().info("raw.%s: %s rows", table, result.rows)
    return result.rows


@task(name="build-silver", retries=1, retry_delay_seconds=30)
def build_silver(batch_id: str) -> dict[str, int]:
    counts = pipeline.run_silver(get_spark(), Settings.from_env(), batch_id)
    get_run_logger().info("silver row counts: %s", counts)
    return counts


@task(name="data-quality")
def data_quality(batch_id: str, fail_on_error: bool) -> dict[str, int]:
    results = pipeline.run_quality(get_spark(), Settings.from_env(), batch_id, fail_on_error)
    return {
        "errors": sum(1 for r in results if r.severity == "error" and not r.passed),
        "warnings": sum(1 for r in results if r.severity == "warn" and not r.passed),
    }


@flow(name="olist-raw-to-silver", log_prints=True)
def olist_pipeline(batch_id: str | None = None, fail_on_dq_error: bool = True) -> dict:
    batch_id = batch_id or pipeline.default_batch_id()
    raw_counts = {t: ingest_table(t, batch_id) for t in SOURCE_FILES}
    silver_counts = build_silver(batch_id)
    dq = data_quality(batch_id, fail_on_dq_error)
    return {"batch_id": batch_id, "raw": raw_counts, "silver": silver_counts, "dq": dq}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--no-fail", action="store_true")
    a = ap.parse_args()
    print(olist_pipeline(batch_id=a.batch_id, fail_on_dq_error=not a.no_fail))
