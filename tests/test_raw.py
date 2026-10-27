import pytest

from olist_pipeline import raw
from olist_pipeline.config import Settings
from olist_pipeline.pipeline import SILVER_TABLES, run_ingest, run_quality, run_silver
from olist_pipeline.schemas import SOURCE_COLUMNS


def test_reads_multiline_quoted_field_and_strips_bom(spark, tmp_path):
    path = tmp_path / "reviews.csv"
    cols = SOURCE_COLUMNS["reviews"]
    header = ",".join(cols)  # like the real translation file: BOM + unquoted header
    # one review whose comment spans two lines and contains a comma and an escaped quote
    row = '"r1","o1",4,,"nice\nproduct, said ""wow""","2018-01-18 00:00:00","2018-01-18 21:46:59"'
    path.write_text("﻿" + header + "\n" + row + "\n", encoding="utf-8")

    df = raw.read_source_csv(spark, str(path), cols)
    rows = df.collect()

    assert len(rows) == 1
    assert rows[0]["review_comment_message"] == 'nice\nproduct, said "wow"'
    assert rows[0]["review_comment_title"] is None
    assert df.columns == cols  # BOM did not leak into the first column name


def test_missing_column_is_schema_drift(spark, tmp_path):
    path = tmp_path / "sellers.csv"
    path.write_text("seller_id,seller_zip_code_prefix,seller_city\ns1,13023,campinas\n")
    with pytest.raises(raw.SchemaDriftError, match="seller_state"):
        raw.read_source_csv(spark, str(path), SOURCE_COLUMNS["sellers"])


def test_extra_column_is_tolerated(spark, tmp_path):
    path = tmp_path / "sellers.csv"
    path.write_text(
        "seller_id,seller_zip_code_prefix,seller_city,seller_state,new_col\ns1,13023,campinas,SP,x\n"
    )
    df = raw.read_source_csv(spark, str(path), SOURCE_COLUMNS["sellers"])
    assert df.columns == SOURCE_COLUMNS["sellers"]


def _write_tiny_source(src):
    """A minimal but referentially valid dataset for every table."""
    def write(name, cols, rows):
        lines = [",".join(cols)] + [",".join(r) for r in rows]
        (src / name).write_text("\n".join(lines) + "\n")

    write("olist_customers_dataset.csv", SOURCE_COLUMNS["customers"], [["c1", "u1", "01037", "sao paulo", "SP"]])
    write("olist_sellers_dataset.csv", SOURCE_COLUMNS["sellers"], [["s1", "01037", "sao paulo", "SP"]])
    write("olist_geolocation_dataset.csv", SOURCE_COLUMNS["geolocation"],
          [["01037", "-23.5", "-46.6", "sao paulo", "SP"], ["01037", "-23.5", "-46.6", "sao paulo", "SP"]])
    write("product_category_name_translation.csv", SOURCE_COLUMNS["category_translation"], [["beleza_saude", "health_beauty"]])
    write("olist_products_dataset.csv", SOURCE_COLUMNS["products"], [["p1", "beleza_saude", "40", "287", "1", "225", "16", "10", "14"]])
    write("olist_orders_dataset.csv", SOURCE_COLUMNS["orders"],
          [["o1", "c1", "delivered", "2018-01-01 10:00:00", "2018-01-01 11:00:00", "2018-01-02 10:00:00", "2018-01-05 10:00:00", "2018-01-08 00:00:00"]])
    write("olist_order_items_dataset.csv", SOURCE_COLUMNS["order_items"], [["o1", "1", "p1", "s1", "2018-01-03 10:00:00", "10.00", "2.50"]])
    write("olist_order_payments_dataset.csv", SOURCE_COLUMNS["payments"], [["o1", "1", "credit_card", "1", "12.50"]])
    write("olist_order_reviews_dataset.csv", SOURCE_COLUMNS["reviews"], [["r1", "o1", "5", "", "", "2018-01-06 00:00:00", "2018-01-06 10:00:00"]])


def test_end_to_end_local_run_is_idempotent(spark, tmp_path):
    src = tmp_path / "source"
    src.mkdir()
    _write_tiny_source(src)
    settings = Settings(
        source_dir=src,
        lake_raw=str(tmp_path / "lake/raw"),
        lake_landing=str(tmp_path / "lake/landing"),
        lake_silver=str(tmp_path / "lake/silver"),
        lake_gold=str(tmp_path / "lake/gold"),
        dq_report_dir=tmp_path / "reports",
        s3_endpoint_url=None,
        aws_region=None,
        spark_master="local[2]",
        shuffle_partitions=2,
    )

    for _ in range(2):  # the second pass is a rerun of the same batch
        run_ingest(spark, settings, "2026-10-22")
        counts = run_silver(spark, settings, "2026-10-22")
        results = run_quality(spark, settings, "2026-10-22", fail_on_error=True)

    assert set(counts) == set(SILVER_TABLES)
    assert counts["orders"] == 1 and counts["geolocation"] == 1  # exact duplicate removed
    assert counts["geolocation_zip"] == 1
    assert not [r for r in results if r.severity == "error" and not r.passed]
    # rerunning must not duplicate raw rows
    assert spark.read.parquet(settings.raw_path("orders")).count() == 1
    assert (tmp_path / "reports" / "dq_2026-10-22.json").exists()
