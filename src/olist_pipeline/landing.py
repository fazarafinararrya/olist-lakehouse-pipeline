"""Landing layer: turn what Airbyte wrote into the raw contract.

Airbyte lands one Parquet file per sync under landing/<stream>/, keeps the typed columns
from Postgres, and adds four metadata columns. Three things have to happen before this data
can enter the raw layer that the rest of the pipeline already depends on:

1. Old syncs are NOT deleted by the S3 destination, even in overwrite mode, so a stream can
   hold files from several syncs. Only the newest generation of each stream is kept.
2. The column contract is enforced here, exactly as the CSV path enforces it, so a renamed
   or dropped column still fails loudly instead of turning into nulls downstream.
3. Everything is cast back to string, because the raw layer is defined as untyped text.
   Silver does the typing, and keeping that unchanged means silver and all its tests work
   identically whichever ingestion path the data came through.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from olist_pipeline.config import Settings
from olist_pipeline.raw import SchemaDriftError
from olist_pipeline.schemas import SOURCE_COLUMNS

log = logging.getLogger(__name__)

# Metadata columns the Airbyte destination adds to every record.
AIRBYTE_COLUMNS = [
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    "_airbyte_generation_id",
]

# Airbyte stream name (= Postgres table) -> logical table name used by this pipeline.
LANDING_STREAMS: dict[str, str] = {
    "customers": "customers",
    "geolocation": "geolocation",
    "order_items": "order_items",
    "order_payments": "payments",
    "order_reviews": "reviews",
    "orders": "orders",
    "products": "products",
    "sellers": "sellers",
    "product_category_name_translation": "category_translation",
}

# The raw layer stores timestamps in the same text format the CSV export used, so that
# silver's to_timestamp(...) keeps working unchanged.
RAW_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass(frozen=True)
class LandingResult:
    stream: str
    table: str
    generation_id: int
    rows: int
    airbyte_changes: int  # rows Airbyte itself flagged as modified during extraction
    path: str


def latest_generation(df: DataFrame) -> int:
    """The newest sync generation present for this stream."""
    value = df.select(F.max("_airbyte_generation_id")).first()[0]
    if value is None:
        raise ValueError("no _airbyte_generation_id found; is this really Airbyte output?")
    return int(value)


def count_airbyte_changes(df: DataFrame) -> int:
    """Rows where Airbyte reported a problem while reading the source (e.g. a failed cast).

    Airbyte does not fail the sync for these; it records them in _airbyte_meta.changes.
    Counting them here means a silent upstream conversion problem still gets reported.
    """
    if "_airbyte_meta" not in df.columns:
        return 0
    changes = F.col("_airbyte_meta.changes")
    return df.where(changes.isNotNull() & (F.size(changes) > 0)).count()


def to_raw_contract(df: DataFrame, table: str) -> DataFrame:
    """Drop Airbyte metadata, enforce the column contract, and cast everything to string."""
    business_columns = [c for c in df.columns if c not in AIRBYTE_COLUMNS]
    expected = SOURCE_COLUMNS[table]

    missing = [c for c in expected if c not in business_columns]
    if missing:
        raise SchemaDriftError(
            f"landing stream for '{table}': missing expected columns {missing}; "
            f"found {business_columns}"
        )
    extra = [c for c in business_columns if c not in expected]
    if extra:
        log.warning("landing stream for '%s': ignoring unexpected columns %s", table, extra)

    projected = []
    for name in expected:
        if isinstance(df.schema[name].dataType, TimestampType):
            # cast() would produce '2017-10-02 10:56:33.0' for some values; format explicitly
            projected.append(F.date_format(F.col(name), RAW_TIMESTAMP_FORMAT).alias(name))
        else:
            projected.append(F.col(name).cast("string").alias(name))
    return df.select(*projected)


def ingest_stream(
    spark: SparkSession, settings: Settings, stream: str, batch_id: str
) -> LandingResult:
    """Read one landing stream and write it into the raw layer for this batch."""
    table = LANDING_STREAMS[stream]
    source = settings.landing_path(stream)

    landed = spark.read.parquet(source)
    generation = latest_generation(landed)
    current = landed.where(F.col("_airbyte_generation_id") == generation)

    changes = count_airbyte_changes(current)
    if changes:
        log.warning(
            "landing.%s: Airbyte flagged %s row(s) in _airbyte_meta.changes", stream, changes
        )

    df = (
        to_raw_contract(current, table)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(f"airbyte:{stream}"))
        .withColumn("ingest_batch_id", F.lit(batch_id))
    )

    target = settings.raw_path(table)
    df.write.mode("overwrite").partitionBy("ingest_batch_id").parquet(target)

    rows = spark.read.parquet(target).where(F.col("ingest_batch_id") == batch_id).count()
    log.info(
        "landing.%s -> raw.%s batch=%s generation=%s rows=%s", stream, table, batch_id, generation, rows
    )
    return LandingResult(stream, table, generation, rows, changes, target)