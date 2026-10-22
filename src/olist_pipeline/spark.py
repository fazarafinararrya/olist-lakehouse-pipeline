"""SparkSession factory. Works against local disk, MinIO or AWS S3 with no code change."""

from __future__ import annotations

from pyspark.sql import SparkSession

from olist_pipeline.config import Settings

# Versions matched to the Hadoop build that ships with Spark 3.5.x.
# A mismatch here is the classic cause of "NoSuchMethodError" / ClassNotFound on s3a://.
HADOOP_AWS_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

_session: SparkSession | None = None


def build_spark(settings: Settings | None = None, app_name: str = "olist-pipeline") -> SparkSession:
    settings = settings or Settings.from_env()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        # Re-running one batch must only replace that batch's partition (idempotent reruns).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Keep partition values (ingest_batch_id=2026-10-22) as strings, never inferred as dates.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if settings.uses_s3:
        builder = (
            builder.config("spark.jars.packages", HADOOP_AWS_PACKAGES)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
        if settings.s3_endpoint_url:  # MinIO or any S3-compatible store
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(settings.s3_endpoint_url.startswith("https")).lower(),
                )
            )
        elif settings.aws_region:  # real AWS S3
            builder = builder.config(
                "spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com"
            )

    return builder.getOrCreate()


def get_spark() -> SparkSession:
    """Process-wide session (Prefect tasks in one flow run share it)."""
    global _session
    if _session is None:
        _session = build_spark()
    return _session
