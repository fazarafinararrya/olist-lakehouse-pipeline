import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from olist_pipeline.schemas import SOURCE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("olist-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def make_raw(spark):
    """make_raw('orders', [dict, ...]) -> all-string DataFrame with the source columns.

    Missing keys become NULL, so each test only spells out the columns it cares about.
    """

    def _make(table: str, rows: list[dict]) -> DataFrame:
        cols = SOURCE_COLUMNS[table]
        schema = StructType([StructField(c, StringType(), True) for c in cols])
        return spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema)

    return _make
