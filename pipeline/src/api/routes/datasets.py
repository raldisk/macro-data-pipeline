"""
Dataset version and quality endpoints — read-only Postgres.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from src.metadata.repository import get_dataset_quality, list_latest_dataset_versions

router = APIRouter()


@router.get("")
async def list_datasets():
    """Latest dataset_versions record per dataset_name."""
    return await list_latest_dataset_versions()


@router.get("/{name}/quality")
async def dataset_quality(name: str):
    """Quality check results for the latest successful run of a named dataset."""
    rows = await get_dataset_quality(name)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No quality results found for dataset '{name}'"
        )
    return rows
