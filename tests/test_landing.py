from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from olist_pipeline import landing
from olist_pipeline.config import Settings
from olist_pipeline.raw import SchemaDriftError

CHANGE = StructType(
    [
        StructField("field", StringType()),
        StructField("change", StringType()),
        StructField("reason", StringType()),
    ]
)
META = StructType(
    [StructField("sync_id", LongType()), StructField("changes", ArrayType(CHANGE))]
)


def sellers_schema():
    """Airbyte's output shape: metadata first, business columns in the source's own order."""
    return StructType(
        [
            StructField("_airbyte_raw_id", StringType()),
            StructField("_airbyte_extracted_at", TimestampType()),
            StructField("_airbyte_meta", META),
            StructField("_airbyte_generation_id", LongType()),
            StructField("seller_id", StringType()),
            StructField("seller_city", StringType()),
            StructField("seller_state", StringType()),
            StructField("seller_zip_code_prefix", StringType()),
        ]
    )


def sellers_rows(spark, rows):
    return spark.createDataFrame(rows, sellers_schema())


def _row(raw_id, generation, seller_id, changes=None):
    return (
        raw_id,
        datetime(2026, 10, 26, 17, 0, 0),
        (generation, changes or []),
        generation,
        seller_id,
        "campinas",
        "SP",
        "13023",
    )


# ------------------------------------------------------------------ generation handling
def test_latest_generation_is_detected(spark):
    df = sellers_rows(spark, [_row("r1", 3, "old"), _row("r2", 4, "new")])
    assert landing.latest_generation(df) == 4


def test_missing_generation_column_is_an_error(spark):
    df = spark.createDataFrame([("x",)], "seller_id string")
    with pytest.raises(Exception):
        landing.latest_generation(df)


# ------------------------------------------------------------------ airbyte-reported issues
def test_counts_rows_airbyte_flagged(spark):
    flagged = [("seller_city", "NULLED", "SOURCE_SERIALIZATION_ERROR")]
    df = sellers_rows(spark, [_row("r1", 4, "a"), _row("r2", 4, "b", changes=flagged)])
    assert landing.count_airbyte_changes(df) == 1


def test_clean_data_reports_no_changes(spark):
    df = sellers_rows(spark, [_row("r1", 4, "a"), _row("r2", 4, "b")])
    assert landing.count_airbyte_changes(df) == 0


# ------------------------------------------------------------------ the raw contract
def test_metadata_is_dropped_and_columns_follow_the_contract(spark):
    df = landing.to_raw_contract(sellers_rows(spark, [_row("r1", 4, "s1")]), "sellers")

    # contract order, not the order Airbyte happened to produce
    assert df.columns == [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ]
    assert all(f.dataType.simpleString() == "string" for f in df.schema.fields)


def test_missing_column_is_schema_drift(spark):
    df = sellers_rows(spark, [_row("r1", 4, "s1")]).drop("seller_state")
    with pytest.raises(SchemaDriftError, match="seller_state"):
        landing.to_raw_contract(df, "sellers")


def test_typed_columns_become_the_text_shape_silver_expects(spark):
    schema = StructType(
        [
            StructField("_airbyte_generation_id", LongType()),
            StructField("order_id", StringType()),
            StructField("order_item_id", IntegerType()),
            StructField("product_id", StringType()),
            StructField("seller_id", StringType()),
            StructField("shipping_limit_date", TimestampType()),
            StructField("price", DecimalType(12, 2)),
            StructField("freight_value", DecimalType(12, 2)),
        ]
    )
    rows = [
        (
            4,
            "o1",
            1,
            "p1",
            "s1",
            datetime(2017, 9, 19, 9, 45, 35),
            Decimal("58.90"),
            Decimal("13.29"),
        )
    ]
    row = landing.to_raw_contract(spark.createDataFrame(rows, schema), "order_items").collect()[0]

    # the exact format silver's to_timestamp(..., 'yyyy-MM-dd HH:mm:ss') parses
    assert row["shipping_limit_date"] == "2017-09-19 09:45:35"
    assert row["price"] == "58.90"
    assert row["order_item_id"] == "1"


# ------------------------------------------------------------------ end to end
def test_ingest_stream_keeps_only_the_newest_sync(spark, tmp_path):
    landing_dir = tmp_path / "landing"
    # two syncs of the same stream, exactly what the S3 destination leaves behind
    sellers_rows(spark, [_row("r1", 3, "from_old_sync")]).write.parquet(
        str(landing_dir / "sellers"), mode="append"
    )
    sellers_rows(spark, [_row("r2", 4, "from_new_sync")]).write.parquet(
        str(landing_dir / "sellers"), mode="append"
    )

    settings = Settings(
        source_dir=tmp_path,
        lake_raw=str(tmp_path / "raw"),
        lake_landing=str(landing_dir),
        lake_silver=str(tmp_path / "silver"),
        lake_gold=str(tmp_path / "gold"),
        dq_report_dir=tmp_path,
        s3_endpoint_url=None,
        aws_region=None,
        spark_master="local[2]",
        shuffle_partitions=2,
    )

    result = landing.ingest_stream(spark, settings, "sellers", "2026-10-27")

    assert result.generation_id == 4
    assert result.rows == 1  # the older sync is ignored, not counted

    written = spark.read.parquet(settings.raw_path("sellers"))
    assert [r["seller_id"] for r in written.collect()] == ["from_new_sync"]
    assert written.first()["source_file"] == "airbyte:sellers"