"""
Gold data serving layer — reads Parquet directly from S3 via PyArrow.

SERVING_LAYER_REQUIRED: The pipeline writes gold Parquet to S3.
The existing API surfaces only metadata (pipeline_runs, dataset_versions, quality_results).
The dashboard chart needs actual row values (CPI values, FX rates).
This endpoint bridges the gap without exposing Spark internals to the UI.

Routes:
  GET /gold/{dataset_name}/data   → latest partition rows as JSON
  GET /gold/{dataset_name}/latest → latest partition metadata only (fast path)
"""
from __future__ import annotations

import io
from typing import Any

import boto3
import pyarrow.parquet as pq
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from src.metadata.repository import list_latest_dataset_versions
from src.utils.config import settings
from src.utils.logging import get_logger

router = APIRouter()
log = get_logger(__name__)

ALLOWED_DATASETS = {"gold_macro_indicators", "gold_exchange_rates"}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT or None,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION,
    )


def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Split s3://bucket/key/path into (bucket, key).
    Raises ValueError on malformed input.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got: {s3_path!r}")
    without_scheme = s3_path[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Cannot parse bucket/key from: {s3_path!r}")
    return bucket, key


def _read_parquet_from_s3(s3_path: str) -> list[dict[str, Any]]:
    """
    Download a single Parquet file from S3 into memory and return as list of dicts.
    Uses boto3 (already a pipeline dependency) to avoid adding s3fs.
    PyArrow 14 is already pinned in pyproject.toml — no new dep needed.
    """
    bucket, key = _parse_s3_path(s3_path)
    s3 = _s3_client()

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Object not found: {s3_path}")

    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf)

    rows: list[dict[str, Any]] = []
    for batch in table.to_batches():
        for i in range(batch.num_rows):
            row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
            # Coerce date → ISO string for JSON serialisation
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
            rows.append(row)

    return rows


@router.get("/{dataset_name}/data")
async def get_gold_data(dataset_name: str) -> JSONResponse:
    """
    Return all rows from the latest gold partition for the named dataset.

    Reads the s3_path stored in dataset_versions (written by metadata_commit),
    opens the Parquet file via boto3 + PyArrow, and streams records back as JSON.

    This is the serving layer the dashboard chart consumes — it does not expose
    Spark session state or internal pipeline structures.
    """
    if dataset_name not in ALLOWED_DATASETS:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{dataset_name}' not found. "
                   f"Allowed: {sorted(ALLOWED_DATASETS)}",
        )

    versions = await list_latest_dataset_versions()
    match = next((v for v in versions if v["dataset_name"] == dataset_name), None)

    if not match:
        raise HTTPException(
            status_code=404,
            detail=f"No dataset_version record found for '{dataset_name}'. "
                   "Run the pipeline at least once.",
        )

    s3_path: str = match["s3_path"]
    if not s3_path:
        raise HTTPException(
            status_code=503,
            detail=f"Dataset '{dataset_name}' has an empty s3_path. "
                   "The latest run may have been PARTIAL.",
        )

    try:
        rows = _read_parquet_from_s3(s3_path)
    except FileNotFoundError as exc:
        log.warning("gold_s3_missing", dataset=dataset_name, path=s3_path, error=str(exc))
        raise HTTPException(status_code=503, detail=f"Gold file unreachable: {s3_path}")
    except Exception as exc:
        log.error("gold_read_error", dataset=dataset_name, path=s3_path, error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to read gold data from S3.")

    log.info("gold_served", dataset=dataset_name, rows=len(rows), path=s3_path)
    return JSONResponse(content={"dataset": dataset_name, "rows": rows, "count": len(rows)})


@router.get("/{dataset_name}/latest")
async def get_gold_latest_meta(dataset_name: str) -> JSONResponse:
    """
    Fast-path metadata-only endpoint — returns partition_key, row_count, s3_path.
    No S3 read. Used by the dashboard dataset version card.
    """
    if dataset_name not in ALLOWED_DATASETS:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    versions = await list_latest_dataset_versions()
    match = next((v for v in versions if v["dataset_name"] == dataset_name), None)

    if not match:
        raise HTTPException(status_code=404, detail=f"No version found for '{dataset_name}'.")

    # Coerce non-serialisable types
    payload = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in match.items()}
    return JSONResponse(content=payload)
