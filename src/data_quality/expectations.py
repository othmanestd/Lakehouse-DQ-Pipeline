"""
Data quality checks for the e-commerce Lakehouse pipeline.
Implements expectation-based validation inspired by Great Expectations.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, lit, sum as spark_sum
from dataclasses import dataclass


@dataclass
class DQResult:
    check_name: str
    table: str
    passed: bool
    metric: float
    threshold: float
    details: str


def check_not_null(df: DataFrame, column: str, threshold: float = 0.99) -> DQResult:
    """Check that a column has at least threshold non-null rate."""
    total = df.count()
    non_null = df.filter(col(column).isNotNull()).count()
    rate = non_null / total if total > 0 else 0
    return DQResult(
        check_name=f"not_null_{column}",
        table="transactions",
        passed=rate >= threshold,
        metric=round(rate, 4),
        threshold=threshold,
        details=f"{column}: {non_null}/{total} non-null ({rate:.2%})"
    )


def check_unique(df: DataFrame, column: str, threshold: float = 0.98) -> DQResult:
    """Check uniqueness rate of a column."""
    total = df.count()
    distinct = df.select(column).distinct().count()
    rate = distinct / total if total > 0 else 0
    return DQResult(
        check_name=f"unique_{column}",
        table="transactions",
        passed=rate >= threshold,
        metric=round(rate, 4),
        threshold=threshold,
        details=f"{column}: {distinct}/{total} unique ({rate:.2%})"
    )


def check_positive(df: DataFrame, column: str, threshold: float = 0.95) -> DQResult:
    """Check that values in a numeric column are positive."""
    total = df.count()
    positive = df.filter(col(column) > 0).count()
    rate = positive / total if total > 0 else 0
    return DQResult(
        check_name=f"positive_{column}",
        table="transactions",
        passed=rate >= threshold,
        metric=round(rate, 4),
        threshold=threshold,
        details=f"{column}: {positive}/{total} positive ({rate:.2%})"
    )


def check_values_in_set(df: DataFrame, column: str, valid_values: set, threshold: float = 0.95) -> DQResult:
    """Check that column values belong to an expected set."""
    total = df.count()
    valid = df.filter(col(column).isin(list(valid_values))).count()
    rate = valid / total if total > 0 else 0
    return DQResult(
        check_name=f"values_in_set_{column}",
        table="transactions",
        passed=rate >= threshold,
        metric=round(rate, 4),
        threshold=threshold,
        details=f"{column}: {valid}/{total} valid ({rate:.2%})"
    )


def check_range(df: DataFrame, column: str, min_val: float, max_val: float, threshold: float = 0.95) -> DQResult:
    """Check that values fall within an expected range."""
    total = df.count()
    in_range = df.filter((col(column) >= min_val) & (col(column) <= max_val)).count()
    rate = in_range / total if total > 0 else 0
    return DQResult(
        check_name=f"range_{column}",
        table="transactions",
        passed=rate >= threshold,
        metric=round(rate, 4),
        threshold=threshold,
        details=f"{column}: {in_range}/{total} in [{min_val}, {max_val}] ({rate:.2%})"
    )


def run_all_checks(df: DataFrame) -> list:
    """Run all data quality checks and return results."""
    return [
        check_not_null(df, "transaction_id"),
        check_not_null(df, "customer_id"),
        check_not_null(df, "product_id"),
        check_not_null(df, "country", threshold=0.90),
        check_unique(df, "transaction_id"),
        check_positive(df, "quantity"),
        check_positive(df, "total_amount"),
        check_values_in_set(df, "payment_method", {"credit_card", "paypal", "bank_transfer", "crypto"}),
        check_values_in_set(df, "currency", {"EUR", "USD", "GBP", "MAD"}),
        check_range(df, "unit_price", 0.01, 10000.0),
    ]


def print_dq_report(results: list):
    """Print a formatted data quality report."""
    print("=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    score = passed / len(results) * 100 if results else 0
    print(f"Score: {passed}/{len(results)} checks passed ({score:.0f}%)")
    print(f"Status: {'PASS' if failed == 0 else 'FAIL'}")
    print("-" * 70)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.check_name}: {r.metric} (threshold: {r.threshold})")
        if not r.passed:
            print(f"         -> {r.details}")
    print("=" * 70)
