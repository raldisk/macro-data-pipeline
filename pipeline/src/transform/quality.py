"""
Quality engine — Wave 1 contract (apply_checks) + Wave 2 implementation (REGISTRY).

apply_checks() is LOCKED. Its signature and internals must not be modified.
The quality agent implements the four REGISTRY functions only.

Every check function contract:
    def check_<name>(df: DataFrame, config: CheckConfig) -> DataFrame
    - Adds boolean column f"{config.check}_passed"
    - Never filters rows
    - Never modifies existing columns
"""
from __future__ import annotations

import asyncio
import concurrent.futures
from dataclasses import dataclass
from functools import reduce
from typing import Any

import asyncpg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.config import settings
from src.utils.logging import get_logger

log = get_logger(__name__)


# ── Contract types ────────────────────────────────────────────────────────────

@dataclass
class CheckConfig:
    check: str
    columns: list[str] | None = None
    column: str | None = None
    value: int | float | None = None
    min: float | None = None
    max: float | None = None


class HardQualityFailure(Exception):
    def __init__(self, run_id: str, failure_rate: float) -> None:
        self.run_id = run_id
        self.failure_rate = failure_rate
        super().__init__(f"run_id={run_id} failure_rate={failure_rate:.4f} exceeds hard threshold")


# ── LOCKED MECHANISM — do not modify ─────────────────────────────────────────

def apply_checks(df: DataFrame, checks: list[CheckConfig]) -> tuple[DataFrame, DataFrame]:
    """
    Apply all checks from REGISTRY in sequence.

    Each check adds a boolean column f"{check.check}_passed".
    is_valid = AND across all check columns.

    Returns:
        (clean_df, rejected_df)
        clean_df  — check columns stripped, is_valid stripped
        rejected_df — all check columns retained for audit
    """
    for check in checks:
        df = REGISTRY[check.check](df, check)

    check_cols = [f"{c.check}_passed" for c in checks]

    df = df.withColumn(
        "is_valid",
        reduce(lambda a, b: a & b, [F.col(c) for c in check_cols]),
    )

    valid_df    = df.filter("is_valid = true").drop(*check_cols, "is_valid")
    rejected_df = df.filter("is_valid = false")

    return valid_df, rejected_df


# ── REGISTRY implementations (quality agent scope) ────────────────────────────

def check_no_nulls(df: DataFrame, config: CheckConfig) -> DataFrame:
    """Pass if all specified columns are non-null for a row."""
    cols = config.columns or []
    if not cols:
        raise ValueError("check_no_nulls requires 'columns' list")
    condition = reduce(
        lambda a, b: a & b,
        [F.col(c).isNotNull() for c in cols],
    )
    return df.withColumn(f"{config.check}_passed", condition)


def check_row_count_min(df: DataFrame, config: CheckConfig) -> DataFrame:
    """Pass if total row count >= config.value. Applies to entire partition."""
    if config.value is None:
        raise ValueError("check_row_count_min requires 'value'")
    total = df.count()
    passed = total >= config.value
    return df.withColumn(f"{config.check}_passed", F.lit(passed))


def check_value_range(df: DataFrame, config: CheckConfig) -> DataFrame:
    """Pass if config.column value is within [min, max] inclusive."""
    if config.column is None:
        raise ValueError("check_value_range requires 'column'")
    condition = F.lit(True)
    if config.min is not None:
        condition = condition & (F.col(config.column) >= config.min)
    if config.max is not None:
        condition = condition & (F.col(config.column) <= config.max)
    return df.withColumn(f"{config.check}_passed", condition)


def check_schema(df: DataFrame, config: CheckConfig) -> DataFrame:
    """Pass if all required columns are present in the DataFrame schema."""
    required = set(config.columns or [])
    present  = set(df.columns)
    missing  = required - present
    passed   = len(missing) == 0
    if missing:
        log.warning("schema_check_missing_cols", missing=list(missing))
    return df.withColumn(f"{config.check}_passed", F.lit(passed))


REGISTRY: dict[str, Any] = {
    "no_nulls":      check_no_nulls,
    "row_count_min": check_row_count_min,
    "value_range":   check_value_range,
    "schema":        check_schema,
}


# ── Silver → Gold column mapping ──────────────────────────────────────────────
# Applied BEFORE apply_checks so quality check column names match contract schema.

_SILVER_TO_GOLD: dict[str, dict[str, str]] = {
    "gold_macro_indicators": {
        "period_date":  "period",        # DateType
        "series_code":  "indicator_code",
        # value, source unchanged
    },
    "gold_exchange_rates": {
        "period_date": "period",         # DateType
        # currency_pair, rate, source unchanged
    },
}

_GOLD_KEEP_COLS: dict[str, list[str]] = {
    "gold_macro_indicators": ["period", "indicator_code", "value", "source"],
    "gold_exchange_rates":   ["period", "currency_pair", "rate", "source"],
}


def _map_silver_to_gold(df: DataFrame, dataset: str) -> DataFrame:
    """Rename and prune silver columns to match the gold contract schema.

    This happens before apply_checks so quality predicates reference the
    correct gold column names (e.g., 'indicator_code', not 'series_code').
    """
    renames = _SILVER_TO_GOLD.get(dataset, {})
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    keep = _GOLD_KEEP_COLS.get(dataset)
    if keep:
        df = df.select([c for c in keep if c in df.columns])

    return df


# ── Async helper for sync Prefect task context ────────────────────────────────

def _run_async_safe(coro: Any) -> Any:
    """Run a coroutine safely regardless of whether an event loop is running."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    return asyncio.run(coro)


# ── Gold write + quarantine (quality agent scope) ─────────────────────────────

def process_quality_and_write_gold(
    silver_path: str,
    contract: Any,          # contracts.loader.Contract
    run_id: str,
    spark_client: Any,      # SparkStorageClient
    batch_date_year: int,
    batch_date_month: int,
) -> str:
    """
    Map silver → gold schema, apply quality checks, write gold and quarantine.

    Returns:
        gold_path (str) — S3 path of written gold partition

    Raises:
        HardQualityFailure — if failure_rate > contract.hard_failure_threshold
    """
    df = spark_client.read_parquet(silver_path)
    total_count = df.count()

    # Map silver column names to gold contract schema before quality checks
    df = _map_silver_to_gold(df, contract.dataset)

    clean_df, rejected_df = apply_checks(df, contract.quality_checks)

    failure_rate = rejected_df.count() / total_count if total_count > 0 else 0.0

    if failure_rate > contract.hard_failure_threshold:
        raise HardQualityFailure(run_id, failure_rate)

    gold_path = (
        f"s3a://{settings.S3_BUCKET}/gold/{contract.dataset}"
        f"/year={batch_date_year}/month={batch_date_month:02d}/"
    )
    spark_client.write_parquet(clean_df, gold_path, contract.partition_key)

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        quarantine_path = (
            f"s3a://{settings.S3_BUCKET}"
            f"/quarantine/{contract.dataset}/{run_id}/"
        )
        spark_client.write_parquet(rejected_df, quarantine_path, [])
        log.warning(
            "rows_quarantined",
            dataset=contract.dataset,
            run_id=run_id,
            count=rejected_count,
        )

    _run_async_safe(_write_quality_results(run_id, contract, clean_df.count(), rejected_count))

    return gold_path


async def _write_quality_results(
    run_id: str,
    contract: Any,
    clean_count: int,
    rejected_count: int,
) -> None:
    # asyncpg needs postgresql:// not postgresql+asyncpg://
    db_url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(db_url)
    try:
        for check in contract.quality_checks:
            await conn.execute(
                """
                INSERT INTO quality_results (run_id, check_name, passed, failed_count, threshold)
                VALUES ($1, $2, $3, $4, $5)
                """,
                run_id,
                check.check,
                rejected_count == 0,
                rejected_count,
                getattr(contract, "hard_failure_threshold", None),
            )
    finally:
        await conn.close()
