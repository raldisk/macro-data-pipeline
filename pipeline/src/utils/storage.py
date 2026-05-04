"""
Storage protocol split — LOCKED Wave 1 artifact.

RawStorageClient  → ingestion agent only (boto3, zero Spark dependency)
SparkStorageClient → transform + quality agents only (S3A via locked SparkSession)

Cross-usage is a hard contract violation:
  ingestion agent must NEVER import pyspark
  transform/quality agents must NEVER call boto3 directly
"""
from __future__ import annotations

import boto3
from typing import Protocol

from pyspark.sql import DataFrame

from src.utils.config import settings
from src.utils.spark import get_spark_session


class RawStorageClient(Protocol):
    """Ingestion agent only. Zero Spark dependency."""

    def write_raw(self, content: bytes, path: str, content_type: str) -> None: ...
    def read_raw(self, path: str) -> bytes: ...
    def exists(self, path: str) -> bool: ...


class SparkStorageClient(Protocol):
    """Transform and quality agents only."""

    def write_parquet(self, df: DataFrame, path: str, partition_cols: list[str]) -> None: ...
    def read_parquet(self, path: str) -> DataFrame: ...
    def exists(self, path: str) -> bool: ...


# ── Concrete implementations ──────────────────────────────────────────────────

class S3RawStorageClient:
    """boto3-backed RawStorageClient. Ingestion agent only."""

    def __init__(self) -> None:
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT or None,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_DEFAULT_REGION,
        )
        self._bucket = settings.S3_BUCKET

    def write_raw(self, content: bytes, path: str, content_type: str = "application/octet-stream") -> None:
        self._s3.put_object(Bucket=self._bucket, Key=path, Body=content, ContentType=content_type)

    def read_raw(self, path: str) -> bytes:
        obj = self._s3.get_object(Bucket=self._bucket, Key=path)
        return obj["Body"].read()

    def exists(self, path: str) -> bool:
        try:
            self._s3.head_object(Bucket=self._bucket, Key=path)
            return True
        except self._s3.exceptions.ClientError:
            return False


class S3SparkStorageClient:
    """S3A-backed SparkStorageClient. Transform and quality agents only."""

    def __init__(self, app_name: str = "storage") -> None:
        self._spark = get_spark_session(app_name)
        self._bucket = settings.S3_BUCKET

    def _s3a(self, path: str) -> str:
        if path.startswith("s3a://"):
            return path
        return f"s3a://{self._bucket}/{path}"

    def write_parquet(self, df: DataFrame, path: str, partition_cols: list[str]) -> None:
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(self._s3a(path))

    def read_parquet(self, path: str) -> DataFrame:
        return self._spark.read.parquet(self._s3a(path))

    def exists(self, path: str) -> bool:
        try:
            self._spark.read.parquet(self._s3a(path))
            return True
        except Exception:
            return False
