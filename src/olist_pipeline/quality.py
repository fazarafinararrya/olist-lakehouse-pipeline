"""Data quality checks on silver tables.

Severity model
- error: breaks a business key or referential rule -> the pipeline run fails.
- warn : a known upstream issue we flag but tolerate (counted and reported, never silent).
Every run writes a JSON report so failures can be investigated afterwards (RCA input).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
]


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    table: str
    check: str
    severity: str  # "error" | "warn"
    passed: bool
    failed_rows: int
    detail: str = ""


def _result(table, check, severity, failed, detail="") -> CheckResult:
    return CheckResult(table, check, severity, failed == 0, int(failed), detail)


def check_row_count_min(df: DataFrame, table: str, minimum: int = 1, severity="error"):
    n = df.count()
    return CheckResult(table, f"row_count>={minimum}", severity, n >= minimum, 0 if n >= minimum else minimum - n, f"rows={n}")


def check_not_null(df: DataFrame, table: str, cols: list[str], severity="error") -> CheckResult:
    any_null = F.lit(False)
    for c in cols:
        any_null = any_null | F.col(c).isNull()
    return _result(table, f"not_null({','.join(cols)})", severity, df.where(any_null).count())


def check_unique(df: DataFrame, table: str, cols: list[str], severity="error") -> CheckResult:
    dup_groups = df.groupBy(*cols).count().where(F.col("count") > 1).count()
    return _result(table, f"unique({','.join(cols)})", severity, dup_groups, "duplicated keys")


def check_accepted_values(df, table, col, allowed, severity="error") -> CheckResult:
    bad = df.where(F.col(col).isNull() | ~F.col(col).isin(list(allowed))).count()
    return _result(table, f"accepted_values({col})", severity, bad)


def check_range(df, table, col, min_value=None, max_value=None, severity="error") -> CheckResult:
    cond = F.lit(False)
    if min_value is not None:
        cond = cond | (F.col(col) < min_value)
    if max_value is not None:
        cond = cond | (F.col(col) > max_value)
    return _result(table, f"range({col},{min_value},{max_value})", severity, df.where(cond).count())


def check_relationship(
    child: DataFrame, parent: DataFrame, table: str, child_col: str, parent_col: str, parent_name: str,
    severity="error",
) -> CheckResult:
    """Distinct child keys with no match in the parent (orphans)."""
    parent_keys = parent.select(F.col(parent_col).alias(child_col)).distinct()
    orphans = child.select(child_col).where(F.col(child_col).isNotNull()).distinct().join(
        parent_keys, on=child_col, how="left_anti"
    )
    return _result(table, f"fk({child_col}->{parent_name}.{parent_col})", severity, orphans.count(), "orphan keys")


def check_flag_count(df: DataFrame, table: str, flag_col: str, severity="warn") -> CheckResult:
    return _result(table, f"flag({flag_col})", severity, df.where(F.col(flag_col)).count(), "rows flagged")


def run_silver_checks(t: dict[str, DataFrame]) -> list[CheckResult]:
    """t: silver table name -> DataFrame."""
    r: list[CheckResult] = []

    for name in ["orders", "customers", "sellers", "order_items", "payments", "reviews", "products", "geolocation_zip"]:
        r.append(check_row_count_min(t[name], name))

    # keys
    r += [
        check_not_null(t["orders"], "orders", ["order_id", "customer_id"]),
        check_unique(t["orders"], "orders", ["order_id"]),
        check_unique(t["customers"], "customers", ["customer_id"]),
        check_unique(t["sellers"], "sellers", ["seller_id"]),
        check_unique(t["products"], "products", ["product_id"]),
        check_unique(t["order_items"], "order_items", ["order_id", "order_item_id"]),
        check_unique(t["payments"], "payments", ["order_id", "payment_sequential"]),
        check_unique(t["reviews"], "reviews", ["review_id", "order_id"]),
        check_unique(t["geolocation_zip"], "geolocation_zip", ["zip_code_prefix"]),
    ]

    # domain rules
    r += [
        check_accepted_values(t["orders"], "orders", "order_status", ORDER_STATUSES),
        check_accepted_values(t["reviews"], "reviews", "review_score", [1, 2, 3, 4, 5]),
        check_range(t["order_items"], "order_items", "price", min_value=0),
        check_range(t["order_items"], "order_items", "freight_value", min_value=0),
        check_range(t["payments"], "payments", "payment_value", min_value=0),
    ]

    # referential integrity
    r += [
        check_relationship(t["orders"], t["customers"], "orders", "customer_id", "customer_id", "customers"),
        check_relationship(t["order_items"], t["orders"], "order_items", "order_id", "order_id", "orders"),
        check_relationship(t["order_items"], t["products"], "order_items", "product_id", "product_id", "products"),
        check_relationship(t["order_items"], t["sellers"], "order_items", "seller_id", "seller_id", "sellers"),
        check_relationship(t["payments"], t["orders"], "payments", "order_id", "order_id", "orders"),
        check_relationship(t["reviews"], t["orders"], "reviews", "order_id", "order_id", "orders"),
        # geocoding coverage is incomplete upstream -> warn only
        check_relationship(t["customers"], t["geolocation_zip"], "customers", "customer_zip_code_prefix", "zip_code_prefix", "geolocation_zip", severity="warn"),
        check_relationship(t["sellers"], t["geolocation_zip"], "sellers", "seller_zip_code_prefix", "zip_code_prefix", "geolocation_zip", severity="warn"),
    ]

    # known upstream issues: tolerated but always reported
    r += [
        check_flag_count(t["orders"], "orders", "dq_delivered_without_date"),
        check_flag_count(t["orders"], "orders", "dq_date_without_delivered_status"),
        check_flag_count(t["payments"], "payments", "dq_zero_value"),
        check_flag_count(t["payments"], "payments", "dq_zero_installments"),
        check_flag_count(t["payments"], "payments", "dq_type_not_defined"),
        check_accepted_values(t["products"], "products", "category_translation_source", ["official", "manual_override", "missing"], severity="warn"),
    ]
    return r


def write_report(results: list[CheckResult], report_dir: Path, batch_id: str) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"dq_{batch_id}.json"
    payload = {
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": sum(1 for x in results if x.severity == "error" and not x.passed),
        "warnings": sum(1 for x in results if x.severity == "warn" and not x.passed),
        "checks": [asdict(x) for x in results],
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def raise_on_errors(results: list[CheckResult]) -> None:
    failed = [x for x in results if x.severity == "error" and not x.passed]
    if failed:
        lines = "\n".join(f"  - {x.table}: {x.check} -> {x.failed_rows} ({x.detail})" for x in failed)
        raise DataQualityError(f"{len(failed)} data quality check(s) failed:\n{lines}")
