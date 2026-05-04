"""
SparkSession factory — LOCKED Wave 1 artifact.
No agent calls SparkSession.builder directly.
All Spark usage routes through this function to guarantee config consistency.
"""
from pyspark.sql import SparkSession
from src.utils.config import settings


def get_spark_session(app_name: str) -> SparkSession:
    """
    Returns (or reuses) a configured SparkSession.

    partitionOverwriteMode=dynamic is non-negotiable:
    without it, mode="overwrite" destroys ALL partitions, not just the target.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
        .getOrCreate()
    )
