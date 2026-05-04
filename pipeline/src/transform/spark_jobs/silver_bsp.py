"""
Silver transform — BSP FX + Policy Rate data.

Reads bronze Parquet, applies dedup + type casting + date normalization,
writes silver Parquet to partitioned S3 path.

BSP-specific sort requirement: always ascending by date after parse.
BSP HTML tables are descending — silver write must correct this.

Prohibited: defining schemas outside schemas.py, calling SparkSession.builder
directly, writing to any Postgres table, calling boto3.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType

from src.transform.schemas import BSP_FX_SILVER_SCHEMA, BSP_POLICY_SILVER_SCHEMA
from src.utils.batch import normalize_batch_date
from src.utils.config import settings
from src.utils.logging import get_logger
from src.utils.spark import get_spark_session

log = get_logger(__name__)

_BSP_FX_DEDUP_KEY     = ["currency_pair", "period_date"]
_BSP_POLICY_DEDUP_KEY = ["decision_date"]


def apply_fx_silver_transform(df: DataFrame) -> DataFrame:
    """BSP FX bronze → silver transformations."""
    # Cast ISO string to date
    df = df.withColumn("period_date", F.col("rate_date").cast(DateType()))

    # Drop null rates (missing values from BSP table)
    df = df.filter(F.col("rate").isNotNull())

    # Cast rate to double (may arrive as string from scraper)
    df = df.withColumn("rate", F.col("rate").cast(DoubleType()))

    # Dedup on natural key
    df = df.dropDuplicates(_BSP_FX_DEDUP_KEY)

    # Enforce ascending order
    df = df.orderBy("period_date")

    # Select silver schema columns only
    silver_cols = [f.name for f in BSP_FX_SILVER_SCHEMA.fields]
    return df.select(*silver_cols)


def apply_policy_silver_transform(df: DataFrame) -> DataFrame:
    """BSP Policy Rate bronze → silver transformations."""
    # Cast decision_date to DateType
    df = df.withColumn("decision_date", F.col("decision_date").cast(DateType()))

    # Validate overnight_rp range
    df = df.filter(
        (F.col("overnight_rp") >= 0.5) & (F.col("overnight_rp") <= 20.0)
    )

    # Validate direction values
    df = df.filter(F.col("direction").isin(["hike", "cut", "hold"]))

    # Filter out future dates
    df = df.filter(F.col("decision_date") <= F.current_date())

    # Dedup
    df = df.dropDuplicates(_BSP_POLICY_DEDUP_KEY)

    # BSP HTML is descending — enforce ascending sort
    df = df.orderBy("decision_date")

    silver_cols = [f.name for f in BSP_POLICY_SILVER_SCHEMA.fields]
    return df.select(*silver_cols)


def transform_to_silver(bronze_path: str, source: str, run_date: date) -> str:
    """
    Read BSP bronze Parquet, transform to silver, write partitioned output.

    Args:
        bronze_path: S3A path to bronze Parquet
        source:      "bsp_fx" or "bsp_policy"
        run_date:    pipeline run date

    Returns:
        silver_path: S3A path to written silver partition
    """
    spark = get_spark_session(f"silver_{source}")
    batch_date = normalize_batch_date(run_date)

    log.info("silver_transform_start", source=source, bronze_path=bronze_path)

    df = spark.read.parquet(bronze_path)

    if source == "bsp_fx":
        df = apply_fx_silver_transform(df)
        # Add year/month columns for Hive-compatible partitioning
        df = df.withColumn("period_year",  F.year("period_date").cast(IntegerType()))
        df = df.withColumn("period_month", F.month("period_date").cast(IntegerType()))
        partition_cols = ["period_year", "period_month"]
    elif source == "bsp_policy":
        df = apply_policy_silver_transform(df)
        df = df.withColumn("decision_year",  F.year("decision_date").cast(IntegerType()))
        df = df.withColumn("decision_month", F.month("decision_date").cast(IntegerType()))
        partition_cols = ["decision_year", "decision_month"]
    else:
        raise ValueError(f"Unknown BSP source: {source}")

    silver_path = (
        f"s3a://{settings.S3_BUCKET}/silver/{source}"
        f"/year={batch_date.year}/month={batch_date.month:02d}/"
    )

    (df.write
       .mode("overwrite")
       .partitionBy(*partition_cols)
       .parquet(silver_path))

    row_count = df.count()
    log.info("silver_transform_done", source=source, silver_path=silver_path, rows=row_count)

    return silver_path
