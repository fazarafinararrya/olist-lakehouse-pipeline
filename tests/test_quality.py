import pytest

from olist_pipeline import quality


def df(spark, rows, cols):
    return spark.createDataFrame(rows, cols)


def test_unique_detects_duplicate_keys(spark):
    data = df(spark, [("a",), ("a",), ("b",)], ["id"])
    result = quality.check_unique(data, "t", ["id"])
    assert not result.passed and result.failed_rows == 1


def test_unique_passes_on_clean_data(spark):
    assert quality.check_unique(df(spark, [("a",), ("b",)], ["id"]), "t", ["id"]).passed


def test_not_null_counts_rows_with_nulls(spark):
    data = df(spark, [("a", 1), (None, 2), ("c", None)], "id string, v int")
    result = quality.check_not_null(data, "t", ["id", "v"])
    assert result.failed_rows == 2


def test_accepted_values(spark):
    data = df(spark, [(1,), (5,), (9,)], ["score"])
    assert quality.check_accepted_values(data, "t", "score", [1, 2, 3, 4, 5]).failed_rows == 1


def test_range_min(spark):
    data = df(spark, [(10.0,), (-1.0,)], ["price"])
    assert quality.check_range(data, "t", "price", min_value=0).failed_rows == 1


def test_relationship_reports_orphan_keys(spark):
    child = df(spark, [("o1",), ("o1",), ("o9",)], ["order_id"])
    parent = df(spark, [("o1",), ("o2",)], ["order_id"])
    result = quality.check_relationship(child, parent, "items", "order_id", "order_id", "orders")
    assert result.failed_rows == 1  # one distinct orphan key (o9)


def test_flag_count(spark):
    data = df(spark, [(True,), (False,), (True,)], ["dq_flag"])
    assert quality.check_flag_count(data, "t", "dq_flag").failed_rows == 2


def test_raise_on_errors_ignores_warnings_but_fails_on_errors():
    warn = quality.CheckResult("t", "c", "warn", False, 3)
    err = quality.CheckResult("t", "c", "error", False, 1, "boom")
    quality.raise_on_errors([warn])  # must not raise
    with pytest.raises(quality.DataQualityError):
        quality.raise_on_errors([warn, err])


def test_write_report_counts_errors_and_warnings(tmp_path):
    results = [
        quality.CheckResult("t", "a", "error", True, 0),
        quality.CheckResult("t", "b", "error", False, 2),
        quality.CheckResult("t", "c", "warn", False, 5),
    ]
    path = quality.write_report(results, tmp_path, "2026-10-22")
    import json

    payload = json.loads(path.read_text())
    assert payload["errors"] == 1 and payload["warnings"] == 1
    assert payload["batch_id"] == "2026-10-22"
