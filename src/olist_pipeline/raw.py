"""Raw layer: land the source CSVs as Parquet, unchanged except for lineage columns.

Design notes
- Everything stays a string in raw. Typing and cleaning happen in silver, so a bad value
  can never fail ingestion and the original text is always replayable.
- Output is partitioned by ingest_batch_id and written with dynamic partition overwrite,
  so re-running a batch replaces only that batch (idempotent rerun).
- The header is validated against schemas.SOURCE_COLUMNS (schema drift fails fast).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from olist_pipeline.config import SOURCE_FILES, Settings
from olist_pipeline.schemas import INGEST_COLUMNS, SOURCE_COLUMNS

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """The source file no longer matches the expected column contract."""


@dataclass(frozen=True)
class IngestResult:
    table: str
    batch_id: str
    rows: int
    path: str


def read_source_csv(spark: SparkSession, path: str, expected_columns: list[str]) -> DataFrame:
    """Read a CSV as all-string columns and enforce the expected header.

    - multiLine/quote/escape: review comments contain line breaks inside quoted fields.
      (multiLine makes each file a single read task; fine at this size, a tuning topic at scale.)
    - A UTF-8 BOM (present in the category translation file) is stripped from the header.
    """
    df = spark.read.options(
        header=True,
        multiLine=True,
        quote='"',
        escape='"',
        mode="PERMISSIVE",
        encoding="UTF-8",
    ).csv(path)

    df = df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])

    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise SchemaDriftError(
            f"{path}: missing expected columns {missing}; found {df.columns}"
        )
    extra = [c for c in df.columns if c not in expected_columns]
    if extra:
        log.warning("%s: ignoring unexpected extra columns %s", path, extra)

    return df.select(*expected_columns)


def ingest_table(
    spark: SparkSession, settings: Settings, table: str, batch_id: str
) -> IngestResult:
    source = settings.source_dir / SOURCE_FILES[table]
    if not Path(source).exists():
        raise FileNotFoundError(f"Source file for '{table}' not found: {source}")

    df = (
        read_source_csv(spark, str(source), SOURCE_COLUMNS[table])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(SOURCE_FILES[table]))
        .withColumn("ingest_batch_id", F.lit(batch_id))
    )
    assert set(INGEST_COLUMNS) <= set(df.columns)

    target = settings.raw_path(table)
    df.write.mode("overwrite").partitionBy("ingest_batch_id").parquet(target)

    rows = spark.read.parquet(target).where(F.col("ingest_batch_id") == batch_id).count()
    log.info("raw.%s batch=%s rows=%s -> %s", table, batch_id, rows, target)
    return IngestResult(table=table, batch_id=batch_id, rows=rows, path=target)


def read_raw(spark: SparkSession, settings: Settings, table: str, batch_id: str) -> DataFrame:
    """Read one batch of a raw table, without the lineage columns."""
    return (
        spark.read.parquet(settings.raw_path(table))
        .where(F.col("ingest_batch_id") == batch_id)
        .drop(*INGEST_COLUMNS)
    )
