"""
Prefect task definitions — LOCKED signatures.

The orchestration agent reads these signatures from frozen Wave 2 output.
No agent invents a parameter. No agent adds a return type not listed here.

All tasks are async — Prefect 3 natively supports async tasks,
avoiding the asyncio event-loop re-entry problem that arises when
sync Prefect tasks call asyncio.get_event_loop().run_until_complete().
"""
from __future__ import annotations

from datetime import date

from prefect import task

from src.utils.logging import get_logger

log = get_logger(__name__)


@task(name="ingest_psa", retries=2, retry_delay_seconds=30)
async def ingest_psa(run_date: date, run_id: str) -> str:
    """Fetch PSA CPI data, write bronze Parquet. Returns bronze S3 path."""
    from src.ingest.landing import ingest_source
    return await ingest_source("psa", run_date)


@task(name="ingest_bsp", retries=2, retry_delay_seconds=30)
async def ingest_bsp(run_date: date, run_id: str) -> str:
    """Fetch BSP FX data, write bronze Parquet. Returns bronze S3 path."""
    from src.ingest.landing import ingest_source
    return await ingest_source("bsp_fx", run_date)


@task(name="transform_psa", retries=2, retry_delay_seconds=30)
def transform_psa(bronze_path: str, run_date: date, run_id: str) -> str:
    """Read PSA bronze, write silver Parquet. Returns silver S3 path."""
    from src.transform.spark_jobs.silver_psa import transform_to_silver
    return transform_to_silver(bronze_path, "psa", run_date)


@task(name="transform_bsp", retries=2, retry_delay_seconds=30)
def transform_bsp(bronze_path: str, run_date: date, run_id: str) -> str:
    """Read BSP bronze, write silver Parquet. Returns silver S3 path."""
    from src.transform.spark_jobs.silver_bsp import transform_to_silver
    return transform_to_silver(bronze_path, "bsp_fx", run_date)


@task(name="run_quality", retries=2, retry_delay_seconds=30)
def run_quality(silver_path: str, dataset_name: str, run_id: str, run_date: date) -> str:
    """Apply quality checks, write gold Parquet, quarantine rejects. Returns gold S3 path."""
    from src.contracts.loader import load_contract
    from src.transform.quality import process_quality_and_write_gold
    from src.utils.batch import normalize_batch_date
    from src.utils.storage import S3SparkStorageClient

    contract     = load_contract(dataset_name)
    spark_client = S3SparkStorageClient(app_name="quality")
    batch_date   = normalize_batch_date(run_date)   # use run_date, not today

    return process_quality_and_write_gold(
        silver_path      = silver_path,
        contract         = contract,
        run_id           = run_id,
        spark_client     = spark_client,
        batch_date_year  = batch_date.year,
        batch_date_month = batch_date.month,
    )


@task(name="metadata_commit")
async def metadata_commit(
    run_id: str,
    batch_id: str,
    source: str,
    run_date: date,
    gold_path: str,
    gold_df_schema_hash: str,
    gold_row_count: int,
    status: str,
) -> None:
    """
    Write pipeline_runs, processed_batches, dataset_versions.
    NEVER writes data — metadata only.
    """
    from src.metadata.repository import (
        insert_dataset_version,
        mark_batch_processed,
        update_pipeline_run,
    )

    await update_pipeline_run(
        run_id           = run_id,
        status           = status,
        records_ingested = gold_row_count,
    )

    await mark_batch_processed(source, run_date, run_id)

    await insert_dataset_version(
        run_id        = run_id,
        dataset_name  = f"gold_{source}",
        partition_key = f"year={run_date.year}/month={run_date.month:02d}",
        row_count     = gold_row_count,
        schema_hash   = gold_df_schema_hash,
        s3_path       = gold_path,
    )

    log.info(
        "metadata_committed",
        run_id    = run_id,
        source    = source,
        status    = status,
        gold_path = gold_path,
    )
