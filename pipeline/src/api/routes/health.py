"""
Health endpoint — checks Postgres connectivity and S3 reachability.
"""
from __future__ import annotations

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter
from fastapi.responses import JSONResponse

import asyncpg

from src.utils.config import settings

router = APIRouter()


@router.get("/health")
async def health_check() -> JSONResponse:
    db_status      = "connected"
    storage_status = "reachable"
    overall        = "ok"

    # Postgres check
    try:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        await conn.execute("SELECT 1")
        await conn.close()
    except Exception as exc:
        db_status = f"error: {exc}"
        overall   = "degraded"

    # S3/MinIO check
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url          = settings.S3_ENDPOINT or None,
            aws_access_key_id     = settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
            region_name           = settings.AWS_DEFAULT_REGION,
        )
        s3.head_bucket(Bucket=settings.S3_BUCKET)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "404":
            storage_status = f"bucket_missing: {settings.S3_BUCKET}"
            overall        = "degraded"
        else:
            storage_status = f"error: {exc}"
            overall        = "degraded"
    except Exception as exc:
        storage_status = f"error: {exc}"
        overall        = "degraded"

    status_code = 200 if overall == "ok" else 503
    return JSONResponse(
        status_code = status_code,
        content     = {"status": overall, "db": db_status, "storage": storage_status},
    )
