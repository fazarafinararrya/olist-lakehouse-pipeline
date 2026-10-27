"""Runtime configuration, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# logical table name -> source CSV file name
SOURCE_FILES: dict[str, str] = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def normalize_uri(uri: str) -> str:
    """Hadoop's S3A connector needs s3a://; accept s3:// as a convenience."""
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://") :]
    return uri


def join_uri(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])


@dataclass(frozen=True)
class Settings:
    source_dir: Path
    lake_raw: str
    lake_landing: str   
    lake_silver: str
    lake_gold: str
    dq_report_dir: Path
    s3_endpoint_url: str | None
    aws_region: str | None
    spark_master: str
    shuffle_partitions: int
    glue_database_silver: str = "olist_silver"
    glue_database_gold: str = "olist_gold"

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            source_dir=Path(env("SOURCE_DIR", "data/source")),
            lake_raw=normalize_uri(env("LAKE_RAW", "data/lake/raw")),
            lake_landing=normalize_uri(
                env("LAKE_LANDING") or join_uri(env("LAKE_RAW", "data/lake/raw"), "landing")
            ),
            lake_silver=normalize_uri(env("LAKE_SILVER", "data/lake/silver")),
            lake_gold=normalize_uri(env("LAKE_GOLD", "data/lake/gold")),
            dq_report_dir=Path(env("DQ_REPORT_DIR", "data/reports")),
            s3_endpoint_url=env("S3_ENDPOINT_URL") or None,
            aws_region=env("AWS_REGION") or None,
            spark_master=env("SPARK_MASTER", "local[*]"),
            shuffle_partitions=int(env("SPARK_SHUFFLE_PARTITIONS", "8")),
            glue_database_silver=env("GLUE_DATABASE_SILVER", "olist_silver"),
            glue_database_gold=env("GLUE_DATABASE_GOLD", "olist_gold"),
        )

    @property
    def uses_s3(self) -> bool:
        return any(u.startswith("s3a://") for u in (self.lake_raw, self.lake_silver, self.lake_gold))

    def raw_path(self, table: str) -> str:
        return join_uri(self.lake_raw, table)
    
    def landing_path(self, stream: str) -> str:
        return join_uri(self.lake_landing, stream)
    
    def silver_path(self, table: str) -> str:
        return join_uri(self.lake_silver, table)
