import pytest
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from olist_pipeline import catalog


def test_type_mapping_covers_the_types_silver_produces():
    assert catalog.spark_type_to_glue(StringType()) == "string"
    assert catalog.spark_type_to_glue(IntegerType()) == "int"
    assert catalog.spark_type_to_glue(BooleanType()) == "boolean"
    assert catalog.spark_type_to_glue(TimestampType()) == "timestamp"
    assert catalog.spark_type_to_glue(DecimalType(12, 2)) == "decimal(12,2)"


def test_unmapped_type_is_rejected_loudly():
    with pytest.raises(catalog.UnsupportedColumnTypeError):
        catalog.spark_type_to_glue(MapType(StringType(), StringType()))


def test_s3a_is_rewritten_for_glue():
    assert catalog.s3a_to_s3("s3a://bucket/silver/orders") == "s3://bucket/silver/orders"
    assert catalog.s3a_to_s3("s3://bucket/x") == "s3://bucket/x"


def test_table_input_shape():
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("price", DecimalType(12, 2)),
            StructField("is_late", BooleanType()),
        ]
    )
    table_input = catalog.build_table_input("orders", schema, "s3a://bucket/silver/orders")

    assert table_input["Name"] == "orders"
    assert table_input["TableType"] == "EXTERNAL_TABLE"
    assert table_input["Parameters"]["classification"] == "parquet"
    sd = table_input["StorageDescriptor"]
    assert sd["Location"] == "s3://bucket/silver/orders/"  # s3:// and a trailing slash
    assert sd["Columns"] == [
        {"Name": "order_id", "Type": "string"},
        {"Name": "price", "Type": "decimal(12,2)"},
        {"Name": "is_late", "Type": "boolean"},
    ]
    assert sd["SerdeInfo"]["SerializationLibrary"] == catalog.PARQUET_SERDE


class _AlreadyExists(Exception):
    pass


class FakeGlue:
    """Minimal stand-in for boto3's Glue client."""

    class exceptions:  # noqa: N801 - mirrors the boto3 client attribute
        AlreadyExistsException = _AlreadyExists

    def __init__(self, existing: set[str] | None = None):
        self.existing = existing or set()
        self.created: list[str] = []
        self.updated: list[str] = []

    def create_table(self, DatabaseName, TableInput):  # noqa: N803 - boto3 signature
        if TableInput["Name"] in self.existing:
            raise _AlreadyExists(TableInput["Name"])
        self.existing.add(TableInput["Name"])
        self.created.append(TableInput["Name"])

    def update_table(self, DatabaseName, TableInput):  # noqa: N803 - boto3 signature
        self.updated.append(TableInput["Name"])


def test_register_table_creates_then_updates():
    glue = FakeGlue()
    schema = StructType([StructField("order_id", StringType())])

    first = catalog.register_table(glue, "olist_silver", "orders", schema, "s3a://b/silver/orders")
    second = catalog.register_table(glue, "olist_silver", "orders", schema, "s3a://b/silver/orders")

    assert first.created is True and glue.created == ["orders"]
    assert second.created is False and glue.updated == ["orders"]
    assert second.location == "s3://b/silver/orders/"


def test_register_silver_tables_uses_parquet_schema(spark, tmp_path):
    from olist_pipeline.config import Settings

    silver_dir = tmp_path / "silver"
    spark.createDataFrame([("o1", 5)], "order_id string, review_score int").write.parquet(
        str(silver_dir / "reviews")
    )
    settings = Settings(
        source_dir=tmp_path,
        lake_raw=str(tmp_path / "raw"),
        lake_silver=str(silver_dir),
        lake_gold=str(tmp_path / "gold"),
        dq_report_dir=tmp_path,
        s3_endpoint_url=None,
        aws_region="ap-southeast-1",
        spark_master="local[2]",
        shuffle_partitions=2,
    )
    glue = FakeGlue()

    registered = catalog.register_silver_tables(spark, settings, ["reviews"], glue_client=glue)

    assert glue.created == ["reviews"]
    assert registered[0].database == "olist_silver"