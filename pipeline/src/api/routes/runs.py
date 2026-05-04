"""
Pipeline run endpoints — read-only Postgres.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.metadata.repository import (
    get_pipeline_run_detail,
    get_run_lineage,
    list_pipeline_runs,
)

router = APIRouter()


@router.get("")
async def list_runs(
    source: Optional[str] = Query(None, description="Filter by source: psa, bsp_fx"),
    limit:  int           = Query(20,   ge=1, le=100),
):
    """List pipeline runs, newest first."""
    return await list_pipeline_runs(source=source, limit=limit)


@router.get("/{run_id}")
async def get_run(run_id: str):
    """Single run with nested stage_metrics and quality_results."""
    detail = await get_pipeline_run_detail(run_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"run_id {run_id} not found")
    return detail


@router.get("/{run_id}/lineage")
async def get_lineage(run_id: str):
    """
    Pipeline run joined with dataset_versions.
    Returns the S3 path of each gold output for this run.
    """
    rows = await get_run_lineage(run_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No lineage found for run_id {run_id}")
    return rows
