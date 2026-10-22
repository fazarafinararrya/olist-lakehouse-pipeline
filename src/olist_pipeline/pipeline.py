"""Library entry points used by both the CLI and the Prefect flow."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from olist_pipeline import catalog, quality, raw, silver
from olist_pipeline.config import SOURCE_FILES, Settings

log = logging.getLogger(__name__)

SILVER_TABLES = [
    "orders", "customers", "sellers", "order_items", "payments", "reviews", "products",
    "geolocation", "geolocation_rejects", "geolocation_zip",
]  # fmt: skip


def default_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run_ingest(
    spark: SparkSession, settings: Settings, batch_id: str, tables: list[str] | None = None
) -> list[raw.IngestResult]:
    return [raw.ingest_table(spark, settings, t, batch_id) for t in (tables or list(SOURCE_FILES))]


def run_silver(spark: SparkSession, settings: Settings, batch_id: str) -> dict[str, int]:
    raw_tables = {t: raw.read_raw(spark, settings, t, batch_id) for t in SOURCE_FILES}
    out: dict[str, int] = {}
    for name, df in silver.build_silver_tables(raw_tables).items():
        path = settings.silver_path(name)
        df.write.mode("overwrite").parquet(path)
        out[name] = spark.read.parquet(path).count()
        log.info("silver.%s rows=%s -> %s", name, out[name], path)
    return out


def run_quality(
    spark: SparkSession, settings: Settings, batch_id: str, fail_on_error: bool = True
) -> list[quality.CheckResult]:
    tables = {name: spark.read.parquet(settings.silver_path(name)) for name in SILVER_TABLES}
    results = quality.run_silver_checks(tables)
    report = quality.write_report(results, settings.dq_report_dir, batch_id)
    errors = sum(1 for r in results if r.severity == "error" and not r.passed)
    warns = sum(1 for r in results if r.severity == "warn" and not r.passed)
    log.info("data quality: %s error(s), %s warning(s) -> %s", errors, warns, report)
    if fail_on_error:
        quality.raise_on_errors(results)
    return results



def run_catalog(spark: SparkSession, settings: Settings) -> list[catalog.RegisteredTable]:
    """Register the silver tables in the Glue Data Catalog (AWS only)."""
    if not settings.lake_silver.startswith("s3a://"):
        raise ValueError(
            "the Glue catalog points at S3 locations; set LAKE_SILVER to an s3a:// URI "
            f"(currently {settings.lake_silver!r})"
        )
    return catalog.register_silver_tables(spark, settings, SILVER_TABLES)
