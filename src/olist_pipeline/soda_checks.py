"""Run Soda Core checks over the silver layer.

Why have this as well as quality.py? They answer to different readers. quality.py is the hard gate
inside the pipeline: it knows about severities, writes a JSON report, and stops the run before bad
data reaches the catalog. Soda keeps its rules in YAML, so someone who does not write Python can
read what is being enforced and add a rule without touching the pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from olist_pipeline.config import Settings

log = logging.getLogger(__name__)

DEFAULT_CHECKS_FILE = Path("/app/soda/checks_silver.yml")

# Silver tables exposed to Soda as temporary views. The view name is what the YAML refers to.
SODA_TABLES = [
    "orders",
    "order_items",
    "payments",
    "reviews",
    "customers",
    "sellers",
    "products",
    "geolocation_zip",
]


class SodaCheckError(RuntimeError):
    """At least one Soda check failed."""


def register_views(spark: SparkSession, settings: Settings) -> None:
    """Expose the silver Parquet tables under the names the checks file uses."""
    for table in SODA_TABLES:
        spark.read.parquet(settings.silver_path(table)).createOrReplaceTempView(table)
        log.debug("soda: registered view %s", table)


def run_scan(
    spark: SparkSession,
    settings: Settings,
    checks_file: Path | None = None,
    fail_on_error: bool = True,
) -> dict:
    """Execute the checks file against the silver layer.

    Soda returns 0 when everything passed, 1 when a check warned, 2 when one failed, and 3 when
    the scan itself errored. Those are turned into a summary here so the caller does not have to
    know Soda's exit codes.
    """
    from soda.scan import Scan  # imported here so a local run without Soda still works

    checks_file = checks_file or DEFAULT_CHECKS_FILE
    if not checks_file.exists():
        raise FileNotFoundError(f"Soda checks file not found: {checks_file}")

    register_views(spark, settings)

    scan = Scan()
    scan.set_scan_definition_name("silver")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")
    scan.add_sodacl_yaml_file(str(checks_file))

    exit_code = scan.execute()
    log.info("soda scan results:\n%s", scan.get_logs_text())

    summary = {
        "exit_code": exit_code,
        "checks_run": len(scan.get_scan_results().get("checks", [])),
        "failed": scan.has_check_fails(),
        "warned": scan.has_check_warns(),
        "errored": scan.has_error_logs(),
    }
    log.info("soda: %s", summary)

    if fail_on_error and (summary["failed"] or summary["errored"]):
        raise SodaCheckError(
            f"Soda reported failures (exit code {exit_code}). See the scan output above."
        )
    return summary