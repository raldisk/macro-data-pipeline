"""
Prefect flow — ph_lakehouse_pipeline.

Execution order is fixed and non-negotiable:
  ingestion → transform → quality → metadata_commit

Task failure uses allow_failure so metadata_commit always fires,
recording PARTIAL status for observability.

Async flow and async tasks — Prefect 3 native; avoids event-loop conflicts.
"""
from __future__ import annotations

from datetime import date

from prefect import allow_failure, flow

from src.metadata.repository import batch_already_processed, create_pipeline_run
from src.orchestrate.tasks import (
    ingest_bsp,
    ingest_psa,
    metadata_commit,
    run_quality,
    transform_bsp,
    transform_psa,
)
from src.utils.batch import make_batch_id, normalize_batch_date
from src.utils.logging import get_logger
from src.utils.schema import compute_schema_hash

log = get_logger(__name__)


@flow(name="ph_lakehouse_pipeline", log_prints=True)
async def lakehouse_pipeline(run_date: date, backfill: bool = False) -> None:
    """
    End-to-end lakehouse pipeline for PSA and BSP data.

    Args:
        run_date:  date to process (normalized to first of month internally)
        backfill:  if True, bypasses processed_batches idempotency guard
    """
    batch_id   = make_batch_id()
    batch_date = normalize_batch_date(run_date)

    for source in ["psa", "bsp"]:
        already_done = await batch_already_processed(source, batch_date)
        if not backfill and already_done:
            log.info("batch_skip", source=source, batch_date=str(batch_date))
            continue

        run_id = await create_pipeline_run(source, run_date)
        log.info("batch_start", source=source, run_id=run_id)

        # ── Stage 1: Ingestion ────────────────────────────────────────────────
        psa_bronze = await ingest_psa(run_date=run_date, run_id=run_id)
        bsp_bronze = await ingest_bsp(run_date=run_date, run_id=run_id)

        # ── Stage 2: Transform ────────────────────────────────────────────────
        psa_silver = transform_psa(bronze_path=psa_bronze, run_date=run_date, run_id=run_id)
        bsp_silver = transform_bsp(bronze_path=bsp_bronze, run_date=run_date, run_id=run_id)

        # ── Stage 3: Quality + Gold write ─────────────────────────────────────
        gold_path_result = allow_failure(run_quality)(
            silver_path  = psa_silver,
            dataset_name = "gold_macro_indicators",
            run_id       = run_id,
            run_date     = run_date,
        )

        gold_path: str | None = gold_path_result if isinstance(gold_path_result, str) else None
        final_status = "SUCCESS" if gold_path else "PARTIAL"
        gold_row_count  = 0
        schema_hash_str = ""

        if gold_path:
            try:
                from src.utils.spark import get_spark_session
                spark            = get_spark_session("hash_check")
                gold_df          = spark.read.parquet(gold_path)
                gold_row_count   = gold_df.count()
                schema_hash_str  = compute_schema_hash(gold_df)
            except Exception as exc:
                log.warning("schema_hash_failed", error=str(exc))

        # ── Stage 4: Metadata commit (always fires) ───────────────────────────
        await metadata_commit(
            run_id              = run_id,
            batch_id            = batch_id,
            source              = source,
            run_date            = run_date,
            gold_path           = gold_path or "",
            gold_df_schema_hash = schema_hash_str,
            gold_row_count      = gold_row_count,
            status              = final_status,
        )

        log.info("batch_complete", source=source, run_id=run_id, status=final_status)
