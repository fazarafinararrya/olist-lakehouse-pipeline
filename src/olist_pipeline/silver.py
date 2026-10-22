"""Silver layer: typed, cleaned, de-duplicated, flagged.

Principles
- Never silently drop business rows. Suspicious rows are kept and marked with dq_* flags
  (so gold can decide), except rows that are unusable by definition (coordinates outside
  Brazil), which go to a rejects table with a reason.
- Every function is a pure DataFrame -> DataFrame transform, so it is unit-testable on
  tiny in-memory data without touching storage.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Open-ended Brazil bounding box; anything outside is a bad geocode.
BRAZIL_LAT = (-34.0, 6.0)
BRAZIL_LNG = (-75.0, -33.0)

# Two categories exist in products but not in the official translation file.
CATEGORY_TRANSLATION_OVERRIDES: dict[str, str] = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_food_preparers",
}

_ACCENTS = "áàâãäéèêëíìîïóòôõöúùûüçñ"
_PLAIN = "aaaaaeeeeiiiiooooouuuucn"


# --------------------------------------------------------------------------- helpers
def _blank_to_null(name: str) -> Column:
    """Trim; empty string becomes NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _ts(name: str) -> Column:
    return F.to_timestamp(F.trim(F.col(name)), TS_FORMAT)


def _text(name: str) -> Column:
    """Lower-case, trimmed, accent-free text (city names)."""
    return F.translate(F.lower(F.trim(F.col(name))), _ACCENTS, _PLAIN)


def _zip5(name: str) -> Column:
    return F.lpad(F.trim(F.col(name)), 5, "0")


# --------------------------------------------------------------------------- tables
def clean_orders(raw: DataFrame) -> DataFrame:
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        *[_ts(c).alias(c) for c in ts_cols],
    )
    delivered = F.coalesce(F.col("order_status") == "delivered", F.lit(False))
    has_delivery_date = F.col("order_delivered_customer_date").isNotNull()
    delay = F.datediff(
        F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")
    )
    return (
        df.withColumn("is_delivered", delivered)
        .withColumn(
            "delivery_days",
            F.when(
                has_delivery_date,
                F.datediff(
                    F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")
                ),
            ),
        )
        .withColumn(
            "delivery_delay_days",
            F.when(has_delivery_date & F.col("order_estimated_delivery_date").isNotNull(), delay),
        )
        .withColumn(
            "is_late",
            F.when(F.col("delivery_delay_days").isNotNull(), F.col("delivery_delay_days") > 0),
        )
        # known upstream inconsistencies, flagged not dropped
        .withColumn("dq_delivered_without_date", delivered & ~has_delivery_date)
        .withColumn("dq_date_without_delivered_status", ~delivered & has_delivery_date)
    )


def clean_customers(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        _zip5("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        _text("customer_city").alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"),
    )


def clean_sellers(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.trim("seller_id").alias("seller_id"),
        _zip5("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        _text("seller_city").alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"),
    )


def clean_order_items(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        F.trim("order_item_id").cast("int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        _ts("shipping_limit_date").alias("shipping_limit_date"),
        F.trim("price").cast("decimal(12,2)").alias("price"),
        F.trim("freight_value").cast("decimal(12,2)").alias("freight_value"),
    )
    return df.withColumn("item_total", F.col("price") + F.col("freight_value"))


def clean_payments(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        F.trim("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.trim("payment_installments").cast("int").alias("payment_installments"),
        F.trim("payment_value").cast("decimal(12,2)").alias("payment_value"),
    )
    return (
        df.withColumn("dq_zero_value", F.col("payment_value") == 0)
        .withColumn("dq_zero_installments", F.col("payment_installments") == 0)
        .withColumn("dq_type_not_defined", F.col("payment_type") == "not_defined")
    )


def clean_reviews(raw: DataFrame) -> DataFrame:
    message = F.regexp_replace(F.col("review_comment_message"), r"\s+", " ")
    df = raw.select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.trim("review_score").cast("int").alias("review_score"),
        _blank_to_null("review_comment_title").alias("review_comment_title"),
        F.when(F.trim(message) == "", F.lit(None))
        .otherwise(F.trim(message))
        .alias("review_comment_message"),
        _ts("review_creation_date").alias("review_creation_date"),
        _ts("review_answer_timestamp").alias("review_answer_timestamp"),
    )
    # review_id alone is NOT unique upstream (same review attached to several orders);
    # the natural key is (review_id, order_id).
    return df.dropDuplicates(["review_id", "order_id"]).withColumn(
        "has_comment", F.col("review_comment_message").isNotNull()
    )


def build_category_mapping(translation_raw: DataFrame) -> DataFrame:
    """Official translation + documented manual overrides for categories it misses."""
    spark = translation_raw.sparkSession
    official = translation_raw.select(
        F.lower(F.trim("product_category_name")).alias("product_category_name"),
        F.trim("product_category_name_english").alias("category_english"),
    ).withColumn("category_translation_source", F.lit("official"))
    overrides = (
        spark.createDataFrame(
            list(CATEGORY_TRANSLATION_OVERRIDES.items()),
            ["product_category_name", "category_english"],
        )
        .join(official.select("product_category_name"), on="product_category_name", how="left_anti")
        .withColumn("category_translation_source", F.lit("manual_override"))
    )
    return official.unionByName(overrides)


def clean_products(raw: DataFrame, translation_raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.trim("product_id").alias("product_id"),
        F.lower(_blank_to_null("product_category_name")).alias("product_category_name"),
        # source misspells these as 'lenght'
        F.trim("product_name_lenght").cast("int").alias("product_name_length"),
        F.trim("product_description_lenght").cast("int").alias("product_description_length"),
        F.trim("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.trim("product_weight_g").cast("int").alias("product_weight_g"),
        F.trim("product_length_cm").cast("int").alias("product_length_cm"),
        F.trim("product_height_cm").cast("int").alias("product_height_cm"),
        F.trim("product_width_cm").cast("int").alias("product_width_cm"),
    )
    mapping = build_category_mapping(translation_raw)
    joined = df.join(F.broadcast(mapping), on="product_category_name", how="left")
    cat = F.col("product_category_name")
    return (
        joined.withColumn(
            "product_category_name_english",
            F.when(cat.isNull(), F.lit("unknown"))
            .when(F.col("category_english").isNull(), cat)
            .otherwise(F.col("category_english")),
        )
        .withColumn(
            "category_translation_source",
            F.when(cat.isNull(), F.lit("missing"))
            .when(F.col("category_translation_source").isNull(), F.lit("untranslated"))
            .otherwise(F.col("category_translation_source")),
        )
        .drop("category_english")
    )


def clean_geolocation(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid_points, rejects). Exact duplicates are removed first."""
    typed = raw.select(
        _zip5("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        F.trim("geolocation_lat").cast("double").alias("latitude"),
        F.trim("geolocation_lng").cast("double").alias("longitude"),
        _text("geolocation_city").alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    ).dropDuplicates()

    in_brazil = F.coalesce(
        F.col("latitude").between(*BRAZIL_LAT) & F.col("longitude").between(*BRAZIL_LNG),
        F.lit(False),
    )
    valid = typed.where(in_brazil)
    rejects = typed.where(~in_brazil).withColumn(
        "reject_reason", F.lit("coordinates_null_or_outside_brazil")
    )
    return valid, rejects


def build_geolocation_zip(valid_points: DataFrame) -> DataFrame:
    """One row per zip prefix: mean coordinates + most frequent city/state.

    Joining customers/sellers to the raw geolocation table would multiply rows
    (up to ~1,100 points per prefix); this table is the safe 1:1 join target.
    """
    coords = valid_points.groupBy("zip_code_prefix").agg(
        F.round(F.avg("latitude"), 6).alias("latitude"),
        F.round(F.avg("longitude"), 6).alias("longitude"),
        F.count(F.lit(1)).alias("n_points"),
    )
    w = Window.partitionBy("zip_code_prefix").orderBy(F.desc("n"), F.asc("city"), F.asc("state"))
    top_place = (
        valid_points.groupBy("zip_code_prefix", "city", "state")
        .agg(F.count(F.lit(1)).alias("n"))
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("zip_code_prefix", "city", "state")
    )
    return coords.join(top_place, on="zip_code_prefix", how="inner")


# --------------------------------------------------------------------------- orchestration
def build_silver_tables(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Map of silver table name -> DataFrame, from a map of raw table name -> DataFrame."""
    geo_valid, geo_rejects = clean_geolocation(raw["geolocation"])
    return {
        "orders": clean_orders(raw["orders"]),
        "customers": clean_customers(raw["customers"]),
        "sellers": clean_sellers(raw["sellers"]),
        "order_items": clean_order_items(raw["order_items"]),
        "payments": clean_payments(raw["payments"]),
        "reviews": clean_reviews(raw["reviews"]),
        "products": clean_products(raw["products"], raw["category_translation"]),
        "geolocation": geo_valid,
        "geolocation_rejects": geo_rejects,
        "geolocation_zip": build_geolocation_zip(geo_valid),
    }
