"""Register silver Parquet tables in the AWS Glue Data Catalog so Athena can query them.

Why not let Spark write straight into the catalog? The Glue catalog client is only bundled
with EMR/Glue runtimes. Registering the table definition with boto3 after Spark has written
the Parquet files keeps the pipeline runnable from a laptop, from CI, and from Glue later on.

The table schema is derived from the Parquet files themselves, so the catalog can never drift
from what was actually written.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

from olist_pipeline.config import Settings

log = logging.getLogger(__name__)

PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

_SIMPLE_TYPES: dict[type[DataType], str] = {
    StringType: "string",
    BooleanType: "boolean",
    ShortType: "smallint",
    IntegerType: "int",
    LongType: "bigint",
    FloatType: "float",
    DoubleType: "double",
    DateType: "date",
    TimestampType: "timestamp",
}


class UnsupportedColumnTypeError(TypeError):
    """A Spark type with no obvious Hive/Athena equivalent reached the catalog."""


def spark_type_to_glue(dtype: DataType) -> str:
    """Translate a Spark type into the Hive type string Glue/Athena expects."""
    if isinstance(dtype, DecimalType):
        return f"decimal({dtype.precision},{dtype.scale})"
    glue_type = _SIMPLE_TYPES.get(type(dtype))
    if glue_type is None:
        raise UnsupportedColumnTypeError(
            f"no Glue type mapping for {dtype.simpleString()!r}; add one in catalog.py"
        )
    return glue_type


def schema_to_columns(schema: StructType) -> list[dict[str, str]]:
    return [{"Name": f.name, "Type": spark_type_to_glue(f.dataType)} for f in schema.fields]


def s3a_to_s3(uri: str) -> str:
    """Glue stores locations as s3://; Spark reads them as s3a://."""
    return "s3://" + uri[len("s3a://") :] if uri.startswith("s3a://") else uri


def build_table_input(table: str, schema: StructType, location: str) -> dict:
    """The Glue TableInput for an external Parquet table."""
    return {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
            "managed_by": "olist-pipeline",
        },
        "StorageDescriptor": {
            "Columns": schema_to_columns(schema),
            "Location": s3a_to_s3(location).rstrip("/") + "/",
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "Compressed": False,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {"serialization.format": "1"},
            },
        },
    }


@dataclass(frozen=True)
class RegisteredTable:
    database: str
    table: str
    location: str
    created: bool  # False means an existing definition was updated


def register_table(
    glue_client, database: str, table: str, schema: StructType, location: str
) -> RegisteredTable:
    """Create the table, or update it if it already exists (idempotent)."""
    table_input = build_table_input(table, schema, location)
    try:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)
        created = True
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
        created = False
    log.info(
        "glue: %s.%s %s -> %s", database, table, "created" if created else "updated", location
    )
    return RegisteredTable(database, table, table_input["StorageDescriptor"]["Location"], created)


def register_silver_tables(
    spark: SparkSession, settings: Settings, tables: list[str], glue_client=None
) -> list[RegisteredTable]:
    """Register every silver table, reading each schema back from its Parquet files."""
    if glue_client is None:
        import boto3  # imported lazily so local runs do not need AWS libraries

        glue_client = boto3.client("glue", region_name=settings.aws_region)

    registered = []
    for table in tables:
        location = settings.silver_path(table)
        schema = spark.read.parquet(location).schema
        registered.append(
            register_table(glue_client, settings.glue_database_silver, table, schema, location)
        )
    return registered