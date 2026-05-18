"""Tests for data quality check functions."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-dq").getOrCreate()


def test_not_null_passes_when_all_present(spark):
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["col1"])
    from src.data_quality.expectations import check_not_null
    result = check_not_null(df, "col1", threshold=1.0)
    assert result.passed is True
    assert result.metric == 1.0


def test_not_null_fails_with_nulls(spark):
    df = spark.createDataFrame([("a",), (None,), ("c",)], ["col1"])
    from src.data_quality.expectations import check_not_null
    result = check_not_null(df, "col1", threshold=0.99)
    assert result.passed is False


def test_unique_detects_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], ["col1"])
    from src.data_quality.expectations import check_unique
    result = check_unique(df, "col1", threshold=0.99)
    assert result.passed is False
    assert result.metric == pytest.approx(0.6667, abs=0.01)


def test_positive_detects_negatives(spark):
    df = spark.createDataFrame([(1,), (-5,), (3,)], ["qty"])
    from src.data_quality.expectations import check_positive
    result = check_positive(df, "qty", threshold=0.99)
    assert result.passed is False
    assert result.metric == pytest.approx(0.6667, abs=0.01)


def test_values_in_set_valid(spark):
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["col1"])
    from src.data_quality.expectations import check_values_in_set
    result = check_values_in_set(df, "col1", {"a", "b", "c"}, threshold=1.0)
    assert result.passed is True


def test_values_in_set_invalid(spark):
    df = spark.createDataFrame([("a",), ("b",), ("x",)], ["col1"])
    from src.data_quality.expectations import check_values_in_set
    result = check_values_in_set(df, "col1", {"a", "b", "c"}, threshold=1.0)
    assert result.passed is False


def test_range_check(spark):
    df = spark.createDataFrame([(10.0,), (50.0,), (999.0,)], ["price"])
    from src.data_quality.expectations import check_range
    result = check_range(df, "price", 0.0, 100.0, threshold=0.99)
    assert result.passed is False
    assert result.metric == pytest.approx(0.6667, abs=0.01)
