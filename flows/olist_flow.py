"""Prefect flow: Airbyte sync -> raw -> silver -> data quality -> Glue catalog -> gold.

Run it:
    docker compose run --rm pipeline python flows/olist_flow.py
    docker compose run --rm pipeline python flows/olist_flow.py --batch-id 2026-10-27
    docker compose run --rm pipeline python flows/olist_flow.py --streams orders,order_items
    docker compose run --rm pipeline python flows/olist_flow.py --skip-sync

Reruns are safe: raw is partitioned by ingest_batch_id with dynamic overwrite, so re-running a
batch replaces only that batch's partition and leaves every other batch untouched.

One Airbyte job syncs every stream enabled on the connection; the API cannot select a subset.
On a memory-constrained host, disable the sync here (--skip-sync) and sync in groups from the
Airbyte UI instead - the landing zone is the hand-off point either way.
"""

from __future__ import annotations

import argparse
import subprocess

from prefect import flow, get_run_logger, task

from olist_pipeline import airbyte, landing, pipeline
from olist_pipeline.config import Settings
from olist_pipeline.spark import get_spark


@task(name="airbyte-sync", retries=1, retry_delay_seconds=60)
def airbyte_sync() -> dict:
    """Refresh the landing zone from the source database, and wait for it to finish.

    One retry only: a second attempt covers a transient API hiccup, but if the sync itself fails
    twice the cause is upstream and further attempts only delay the alert.
    """
    result = airbyte.run_sync()
    get_run_logger().info(
        "airbyte job %s %s in %ss", result["job_id"], result["status"], result["seconds"]
    )
    return result


@task(name="ingest-stream", retries=2, retry_delay_seconds=30)
def ingest_stream(stream: str, batch_id: str) -> dict:
    """One Airbyte stream -> the raw contract. Retried: S3 calls can fail transiently."""
    result = landing.ingest_stream(get_spark(), Settings.from_env(), stream, batch_id)
    get_run_logger().info(
        "raw.%s: %s rows (airbyte generation %s, %s flagged row(s))",
        result.table,
        result.rows,
        result.generation_id,
        result.airbyte_changes,
    )
    return {
        "table": result.table,
        "rows": result.rows,
        "generation_id": result.generation_id,
        "airbyte_changes": result.airbyte_changes,
    }


@task(name="build-silver", retries=1, retry_delay_seconds=30)
def build_silver(batch_id: str) -> dict[str, int]:
    counts = pipeline.run_silver(get_spark(), Settings.from_env(), batch_id)
    get_run_logger().info("silver row counts: %s", counts)
    return counts


@task(name="data-quality")
def data_quality(batch_id: str, fail_on_error: bool) -> dict[str, int]:
    """No retries: a failing check is a data problem, and running it again cannot fix it."""
    results = pipeline.run_quality(get_spark(), Settings.from_env(), batch_id, fail_on_error)
    summary = {
        "errors": sum(1 for r in results if r.severity == "error" and not r.passed),
        "warnings": sum(1 for r in results if r.severity == "warn" and not r.passed),
    }
    get_run_logger().info("data quality: %s", summary)
    return summary


@task(name="register-catalog", retries=2, retry_delay_seconds=15)
def register_catalog() -> int:
    settings = Settings.from_env()
    registered = pipeline.run_catalog(get_spark(), settings)
    get_run_logger().info(
        "glue: %s table(s) registered in %s", len(registered), settings.glue_database_silver
    )
    return len(registered)


@task(name="dbt-build", retries=1, retry_delay_seconds=30)
def dbt_build() -> dict:
    """Build the gold star schema and run its tests.

    dbt is invoked through its CLI rather than imported, because dbt's Python entry point is not
    a stable API. Its output is forwarded to the Prefect log, so a failing model is diagnosable
    from the UI without opening a terminal.
    """
    logger = get_run_logger()
    completed = subprocess.run(
        ["dbt", "build", "--project-dir", "/dbt", "--profiles-dir", "/dbt"],
        capture_output=True,
        text=True,
    )
    for line in completed.stdout.splitlines()[-40:]:
        logger.info("dbt | %s", line)

    if completed.returncode != 0:
        for line in (completed.stderr or completed.stdout).splitlines()[-20:]:
            logger.error("dbt | %s", line)
        raise RuntimeError(f"dbt build failed (exit {completed.returncode})")

    return {"exit_code": completed.returncode}


@flow(name="olist-landing-to-gold", log_prints=True)
def olist_pipeline(
    batch_id: str | None = None,
    streams: list[str] | None = None,
    fail_on_dq_error: bool = True,
    update_catalog: bool = True,
    build_gold: bool = True,
    run_airbyte_sync: bool = True,
) -> dict:
    batch_id = batch_id or pipeline.default_batch_id()
    logger = get_run_logger()
    logger.info("batch %s starting", batch_id)

    # Refresh the landing zone first. Without this the flow would quietly reprocess whatever the
    # last sync left there, which is how a pipeline ends up reporting yesterday's numbers.
    sync = airbyte_sync() if run_airbyte_sync else None

    selected = streams or list(landing.LANDING_STREAMS)
    # Streams are independent of each other, so one failing does not stop the others from being
    # attempted; silver below waits for all of them because it reads every raw table.
    raw_results = [
        ingest_stream(stream, batch_id, wait_for=[sync] if sync else None)
        for stream in selected
    ]

    silver_counts = build_silver(batch_id, wait_for=raw_results)
    dq = data_quality(batch_id, fail_on_dq_error, wait_for=[silver_counts])

    catalog_tables = None
    if update_catalog:
        catalog_tables = register_catalog(wait_for=[dq])

    gold = None
    if build_gold:
        # Waits for the catalog: dbt reads silver through Athena, which resolves those tables from
        # the Glue Data Catalog, so a stale catalog would make dbt model stale definitions.
        gold = dbt_build(wait_for=[catalog_tables] if catalog_tables else [dq])

    summary = {
        "batch_id": batch_id,
        "airbyte": sync,
        "raw": {r["table"]: r["rows"] for r in raw_results},
        "silver": silver_counts,
        "data_quality": dq,
        "catalog_tables": catalog_tables,
        "gold": gold,
    }
    logger.info("batch %s finished: %s", batch_id, summary)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-id", default=None, help="defaults to today (UTC)")
    ap.add_argument("--streams", default=None, help="comma-separated subset, for reruns")
    ap.add_argument("--no-fail", action="store_true", help="report DQ errors without failing")
    ap.add_argument("--skip-catalog", action="store_true")
    ap.add_argument("--skip-gold", action="store_true", help="stop after the catalog, skip dbt")
    ap.add_argument(
        "--skip-sync", action="store_true", help="use whatever is already in the landing zone"
    )
    a = ap.parse_args()

    print(
        olist_pipeline(
            batch_id=a.batch_id,
            streams=a.streams.split(",") if a.streams else None,
            fail_on_dq_error=not a.no_fail,
            update_catalog=not a.skip_catalog,
            build_gold=not a.skip_gold,
            run_airbyte_sync=not a.skip_sync,
        )
    )