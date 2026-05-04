"""
Silver transform — PSA CPI data.

Reads bronze Parquet, applies dedup + type casting + date normalization,
writes silver Parquet to partitioned S3 path.

Prohibited: defining schemas outside schemas.py, calling SparkSession.builder
directly, writing to any Postgres table, calling boto3.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

from src.transform.schemas import PSA_SILVER_SCHEMA
from src.utils.batch import normalize_batch_date
from src.utils.config import settings
from src.utils.logging import get_logger
from src.utils.spark import get_spark_session

log = get_logger(__name__)

# Natural dedup key for PSA silver
_PSA_DEDUP_KEY = ["series_code", "period_date"]


def _parse_psa_period(period_col: str) -> F.Column:
    """
    Convert PSA period string "2024M01" → date(2024, 1, 1).

    Strategy:
      1. Split on "M": ["2024", "01"]
      2. Construct ISO string "2024-01-01"
      3. Cast to DateType
    """
    year_part  = F.split(F.col(period_col), "M").getItem(0)
    month_part = F.split(F.col(period_col), "M").getItem(1)
    iso_str    = F.concat(year_part, F.lit("-"), month_part, F.lit("-01"))
    return iso_str.cast(DateType())


def apply_silver_transform(df: DataFrame, source: str) -> DataFrame:
    """Apply all PSA-specific silver transformations."""
    # Convert period string to date
    df = df.withColumn("period_date", _parse_psa_period("period"))

    # Extract year and month for partitioning
    df = df.withColumn("period_year",  F.year("period_date").cast(IntegerType()))
    df = df.withColumn("period_month", F.month("period_date").cast(IntegerType()))

    # Deduplicate on natural key
    df = df.dropDuplicates(_PSA_DEDUP_KEY)

    return df


def transform_to_silver(bronze_path: str, source: str, run_date: date) -> str:
    """
    Read PSA bronze Parquet, transform to silver, write partitioned output.

    Args:
        bronze_path: S3A path to bronze Parquet
        source:      source identifier ("psa")
        run_date:    pipeline run date

    Returns:
        silver_path: S3A path to written silver partition
    """
    spark = get_spark_session(f"silver_{source}")
    batch_date = normalize_batch_date(run_date)

    log.info("silver_transform_start", source=source, bronze_path=bronze_path)

    df = spark.read.parquet(bronze_path)
    df = apply_silver_transform(df, source)

    # Select silver schema columns + partition columns (year/month)
    silver_cols = [f.name for f in PSA_SILVER_SCHEMA.fields]
    df = df.select(*silver_cols, "period_year", "period_month")

    silver_path = (
        f"s3a://{settings.S3_BUCKET}/silver/{source}"
        f"/year={batch_date.year}/month={batch_date.month:02d}/"
    )

    (df.write
       .mode("overwrite")
       .partitionBy("period_year", "period_month")
       .parquet(silver_path))

    row_count = df.count()
    log.info("silver_transform_done", source=source, silver_path=silver_path, rows=row_count)

    return silver_path
