from __future__ import annotations
import pytest
from pyspark.sql import SparkSession
from src.transform.quality import (
    HardQualityFailure, apply_checks, check_no_nulls,
    check_row_count_min, check_schema, check_value_range, CheckConfig,
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_quality").getOrCreate()

def test_check_no_nulls_all_present(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = check_no_nulls(df, CheckConfig(check="no_nulls", columns=["id", "name"]))
    assert all(r["no_nulls_passed"] is True for r in result.collect())

def test_check_no_nulls_with_null(spark):
    df = spark.createDataFrame([(1, None), (2, "b")], ["id", "name"])
    result = check_no_nulls(df, CheckConfig(check="no_nulls", columns=["name"]))
    rows = sorted(result.collect(), key=lambda r: r["id"])
    assert rows[0]["no_nulls_passed"] is False
    assert rows[1]["no_nulls_passed"] is True

def test_check_row_count_min_passes(spark):
    df = spark.createDataFrame([(i,) for i in range(10)], ["v"])
    result = check_row_count_min(df, CheckConfig(check="row_count_min", value=5))
    assert all(r["row_count_min_passed"] is True for r in result.collect())

def test_check_row_count_min_fails(spark):
    df = spark.createDataFrame([(i,) for i in range(3)], ["v"])
    result = check_row_count_min(df, CheckConfig(check="row_count_min", value=10))
    assert all(r["row_count_min_passed"] is False for r in result.collect())

def test_check_value_range_in_bounds(spark):
    df = spark.createDataFrame([(5.0,)], ["v"])
    result = check_value_range(df, CheckConfig(check="value_range", column="v", min=0.0, max=100.0))
    assert result.collect()[0]["value_range_passed"] is True

def test_check_value_range_out_of_bounds(spark):
    df = spark.createDataFrame([(-1.0,), (200.0,)], ["v"])
    result = check_value_range(df, CheckConfig(check="value_range", column="v", min=0.0, max=100.0))
    assert all(r["value_range_passed"] is False for r in result.collect())

def test_apply_checks_splits_correctly(spark):
    df = spark.createDataFrame([(1, 5.0), (2, None), (3, 150.0)], ["id", "value"])
    checks = [
        CheckConfig(check="no_nulls", columns=["value"]),
        CheckConfig(check="value_range", column="value", min=0.0, max=100.0),
    ]
    clean, rejected = apply_checks(df, checks)
    assert clean.count() == 1
    assert rejected.count() == 2

def test_apply_checks_clean_has_no_check_columns(spark):
    df = spark.createDataFrame([(1, 5.0)], ["id", "value"])
    clean, _ = apply_checks(df, [CheckConfig(check="no_nulls", columns=["value"])])
    assert "no_nulls_passed" not in clean.columns
    assert "is_valid" not in clean.columns
