"""
Metadata repository — raw SQL via asyncpg. No ORM.

Write ownership:
  file_manifest      → ingestion agent (landing.py)
  pipeline_runs      → orchestration agent (here)
  stage_metrics      → orchestration agent (here)
  quality_results    → quality agent (quality.py)
  dataset_versions   → orchestration agent (here)
  processed_batches  → orchestration agent (here)
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

import asyncpg

from src.metadata.models import (
    DatasetVersionRow,
    LineageRow,
    PipelineRunDetail,
    PipelineRunRow,
    QualityResultRow,
    StageMetricRow,
)
from src.utils.batch import normalize_batch_date
from src.utils.config import settings
from src.utils.logging import get_logger

log = get_logger(__name__)


async def get_conn() -> asyncpg.Connection:
    # asyncpg needs postgresql:// scheme; strip SQLAlchemy driver prefix
    url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    return await asyncpg.connect(url)


# ── pipeline_runs ─────────────────────────────────────────────────────────────

async def create_pipeline_run(source: str, run_date: date) -> str:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO pipeline_runs (pipeline_name, source, run_date, status)
            VALUES ($1, $2, $3, 'RUNNING')
            RETURNING run_id::text
            """,
            "ph_lakehouse_pipeline", source, run_date,
        )
        return row["run_id"]
    finally:
        await conn.close()


async def update_pipeline_run(
    run_id: str,
    status: str,
    records_ingested: Optional[int] = None,
    records_rejected: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            UPDATE pipeline_runs
            SET status = $1, ended_at = now(),
                records_ingested = COALESCE($2, records_ingested),
                records_rejected = COALESCE($3, records_rejected),
                error_message    = COALESCE($4, error_message)
            WHERE run_id = $5::uuid
            """,
            status, records_ingested, records_rejected, error_message, run_id,
        )
    finally:
        await conn.close()


async def batch_already_processed(source: str, batch_date: date) -> bool:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_batches WHERE source = $1 AND batch_date = $2",
            source, normalize_batch_date(batch_date),
        )
        return row is not None
    finally:
        await conn.close()


# ── processed_batches ─────────────────────────────────────────────────────────

async def mark_batch_processed(source: str, batch_date: date, run_id: str) -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO processed_batches (source, batch_date, run_id)
            VALUES ($1, $2, $3::uuid)
            ON CONFLICT (source, batch_date) DO UPDATE SET run_id = EXCLUDED.run_id
            """,
            source, normalize_batch_date(batch_date), run_id,
        )
    finally:
        await conn.close()


# ── dataset_versions ──────────────────────────────────────────────────────────

async def insert_dataset_version(
    run_id: str,
    dataset_name: str,
    partition_key: str,
    row_count: int,
    schema_hash: str,
    s3_path: str,
) -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO dataset_versions
                (run_id, dataset_name, partition_key, row_count, schema_hash, s3_path)
            VALUES ($1::uuid, $2, $3, $4, $5, $6)
            """,
            run_id, dataset_name, partition_key, row_count, schema_hash, s3_path,
        )
    finally:
        await conn.close()


# ── Read queries (API layer) ──────────────────────────────────────────────────

async def list_pipeline_runs(
    source: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    conn = await get_conn()
    try:
        if source:
            rows = await conn.fetch(
                "SELECT * FROM pipeline_runs WHERE source = $1 ORDER BY started_at DESC LIMIT $2",
                source, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT $1", limit
            )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def get_pipeline_run_detail(run_id: str) -> Optional[dict]:
    conn = await get_conn()
    try:
        run = await conn.fetchrow(
            "SELECT * FROM pipeline_runs WHERE run_id = $1::uuid", run_id
        )
        if not run:
            return None
        stages = await conn.fetch(
            "SELECT * FROM stage_metrics WHERE run_id = $1::uuid ORDER BY started_at", run_id
        )
        quality = await conn.fetch(
            "SELECT * FROM quality_results WHERE run_id = $1::uuid", run_id
        )
        return {
            **dict(run),
            "stage_metrics":   [dict(s) for s in stages],
            "quality_results": [dict(q) for q in quality],
        }
    finally:
        await conn.close()


async def get_run_lineage(run_id: str) -> list[dict]:
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT pr.run_id, pr.source, pr.run_date,
                   dv.dataset_name, dv.row_count, dv.s3_path
            FROM pipeline_runs pr
            JOIN dataset_versions dv ON dv.run_id = pr.run_id
            WHERE pr.run_id = $1::uuid
            """,
            run_id,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def list_latest_dataset_versions() -> list[dict]:
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (dataset_name)
                id, run_id, dataset_name, partition_key, row_count, schema_hash, s3_path, created_at
            FROM dataset_versions
            ORDER BY dataset_name, created_at DESC
            """
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def get_dataset_quality(name: str) -> list[dict]:
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT qr.*
            FROM quality_results qr
            JOIN pipeline_runs pr ON pr.run_id = qr.run_id
            JOIN dataset_versions dv ON dv.run_id = pr.run_id
            WHERE dv.dataset_name = $1 AND pr.status = 'SUCCESS'
            ORDER BY pr.ended_at DESC
            LIMIT 20
            """,
            name,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()
