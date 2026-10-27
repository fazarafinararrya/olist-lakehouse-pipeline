"""Command line: python -m olist_pipeline.cli {ingest,silver,quality,all} [--batch-id YYYY-MM-DD]."""

from __future__ import annotations

import argparse
import logging

from olist_pipeline import pipeline
from olist_pipeline.config import Settings
from olist_pipeline.spark import build_spark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="olist-pipeline")
    parser.add_argument(
        "stage",
        choices=["landing", "ingest", "silver", "quality", "catalog", "all"],
        help="'landing' is the production path (Airbyte); 'ingest' reads the CSVs offline",
    )
    parser.add_argument("--batch-id", default=None, help="defaults to today (UTC)")
    parser.add_argument(
        "--no-fail", action="store_true", help="report data quality errors without failing"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    batch_id = args.batch_id or pipeline.default_batch_id()
    spark = build_spark(settings)
    try:
        if args.stage in ("landing", "all"):
            pipeline.run_landing_ingest(spark, settings, batch_id)
        if args.stage == "ingest":
            pipeline.run_ingest(spark, settings, batch_id)
        if args.stage in ("silver", "all"):
            pipeline.run_silver(spark, settings, batch_id)
        if args.stage in ("quality", "all"):
            pipeline.run_quality(spark, settings, batch_id, fail_on_error=not args.no_fail)
        if args.stage == "catalog":
            pipeline.run_catalog(spark, settings)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
