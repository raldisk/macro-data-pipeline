from __future__ import annotations
from datetime import datetime, timezone
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder.master("local[1]").appName("test_silver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate())

def test_psa_period_parse(spark):
    from src.transform.spark_jobs.silver_psa import apply_silver_transform
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [("CPI_ALL_ITEMS", "2024M01", 105.2, "index", "psa", ts, "b-001")],
        schema=["series_code", "period", "value", "unit", "source", "fetched_at", "batch_id"],
    )
    row = apply_silver_transform(df, "psa").first()
    assert str(row["period_date"]) == "2024-01-01"
    assert row["period_year"] == 2024
    assert row["period_month"] == 1

def test_psa_silver_dedup(spark):
    from src.transform.spark_jobs.silver_psa import apply_silver_transform
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [("CPI_ALL_ITEMS", "2024M01", 105.2, "index", "psa", ts, "b-001"),
         ("CPI_ALL_ITEMS", "2024M01", 105.5, "index", "psa", ts, "b-002")],
        schema=["series_code", "period", "value", "unit", "source", "fetched_at", "batch_id"],
    )
    assert apply_silver_transform(df, "psa").count() == 1

def test_bsp_fx_null_rates_dropped(spark):
    from src.transform.spark_jobs.silver_bsp import apply_fx_silver_transform
    ts = datetime(2024, 1, 15, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [("2024-01-01", "USD/PHP", 56.3, "bsp", ts, "b-001"),
         ("2024-02-01", "USD/PHP", None, "bsp", ts, "b-001")],
        schema=["rate_date", "currency_pair", "rate", "source", "fetched_at", "batch_id"],
    )
    assert apply_fx_silver_transform(df).count() == 1

def test_dynamic_partition_overwrite_preserves_adjacent(spark, tmp_path):
    base = str(tmp_path / "silver_test")
    df_jan = spark.createDataFrame([("2024-01-01", 105.0)], ["period_date", "value"])
    df_jan.write.mode("overwrite").partitionBy("period_date").parquet(base)
    df_feb = spark.createDataFrame([("2024-02-01", 106.0)], ["period_date", "value"])
    df_feb.write.mode("overwrite").partitionBy("period_date").parquet(base)
    assert spark.read.parquet(base).count() == 2, "Adjacent partition destroyed — partitionOverwriteMode=dynamic not active"
