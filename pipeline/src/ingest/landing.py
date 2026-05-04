"""
Landing module — ingestion agent.

Responsibilities:
  1. SHA-256 dedup via file_manifest (prevents re-download of unchanged data)
  2. Normalize raw records to bronze Parquet via pyarrow
  3. Write bronze bytes to S3 via RawStorageClient
  4. Insert file_manifest record

Prohibited: importing pyspark, calling SparkStorageClient.
"""
from __future__ import annotations

import hashlib
import io
from datetime import date, datetime, timezone
from typing import Any

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingest.fetch_bsp import fetch_bsp_fx_raw, fetch_bsp_policy_raw
from src.ingest.fetch_psa import fetch_psa_raw
from src.utils.batch import make_batch_id, normalize_batch_date
from src.utils.config import settings
from src.utils.logging import get_logger
from src.utils.storage import RawStorageClient, S3RawStorageClient

log = get_logger(__name__)


# ── Postgres helpers ──────────────────────────────────────────────────────────

async def _manifest_exists(source: str, sha256_hash: str, conn: asyncpg.Connection) -> bool:
    row = await conn.fetchrow(
        "SELECT file_path FROM file_manifest WHERE source = $1 AND sha256_hash = $2",
        source, sha256_hash,
    )
    return row is not None


async def _get_existing_path(source: str, sha256_hash: str, conn: asyncpg.Connection) -> str:
    row = await conn.fetchrow(
        "SELECT file_path FROM file_manifest WHERE source = $1 AND sha256_hash = $2",
        source, sha256_hash,
    )
    return row["file_path"]


async def _insert_file_manifest(
    *,
    source: str,
    fetch_date: date,
    file_path: str,
    sha256_hash: str,
    byte_size: int,
    batch_id: str,
    status: str,
    conn: asyncpg.Connection,
) -> None:
    await conn.execute(
        """
        INSERT INTO file_manifest
            (source, fetch_date, file_path, sha256_hash, byte_size, batch_id, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (source, sha256_hash) DO NOTHING
        """,
        source, fetch_date, file_path, sha256_hash, byte_size, batch_id, status,
    )


# ── pyarrow normalization (no Spark — JVM overhead unjustified here) ──────────

def _records_to_parquet_bytes(
    records: list[dict],
    fetched_at: datetime,
    batch_id: str,
) -> bytes:
    """Convert raw record dicts to bronze Parquet bytes via pyarrow."""
    if not records:
        raise ValueError("Cannot write empty record set to bronze")

    table = pa.Table.from_pylist(records)

    # Append metadata columns
    n = len(table)
    table = table.append_column(
        "fetched_at",
        pa.array([fetched_at] * n, type=pa.timestamp("us", tz="UTC")),
    )
    table = table.append_column(
        "batch_id",
        pa.array([batch_id] * n, type=pa.string()),
    )

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    return buf.getvalue().to_pybytes()


# ── Main ingestion entry point ────────────────────────────────────────────────

async def ingest_source(
    source: str,
    run_date: date,
    raw_client: RawStorageClient | None = None,
) -> str:
    """
    Fetch source data, deduplicate via file_manifest, write bronze Parquet.

    Args:
        source:     "psa", "bsp_fx", or "bsp_policy"
        run_date:   pipeline run date
        raw_client: optional injected client (for testing); defaults to S3

    Returns:
        bronze S3 path (str)

    Idempotency:
        Same content hash → skip write → return existing path
    """
    raw_client = raw_client or S3RawStorageClient()
    fetched_at = datetime.now(tz=timezone.utc)

    # Fetch raw records
    if source == "psa":
        records = fetch_psa_raw()
    elif source == "bsp_fx":
        records = fetch_bsp_fx_raw()
    elif source == "bsp_policy":
        records = fetch_bsp_policy_raw()
    else:
        raise ValueError(f"Unknown source: {source}")

    batch_id   = make_batch_id()
    batch_date = normalize_batch_date(run_date)

    parquet_bytes = _records_to_parquet_bytes(records, fetched_at, batch_id)
    sha256        = hashlib.sha256(parquet_bytes).hexdigest()

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        # SHA-256 dedup: same content → skip
        if await _manifest_exists(source, sha256, conn):
            existing_path = await _get_existing_path(source, sha256, conn)
            log.info("ingest_skip_duplicate", source=source, sha256=sha256[:12])
            return existing_path

        path = (
            f"bronze/{source}"
            f"/year={batch_date.year}/month={batch_date.month:02d}"
            f"/{batch_id}.parquet"
        )

        raw_client.write_raw(parquet_bytes, path, content_type="application/octet-stream")

        await _insert_file_manifest(
            source=source,
            fetch_date=run_date,
            file_path=path,
            sha256_hash=sha256,
            byte_size=len(parquet_bytes),
            batch_id=batch_id,
            status="SUCCESS",
            conn=conn,
        )

        log.info(
            "ingest_complete",
            source=source,
            path=path,
            rows=len(records),
            bytes=len(parquet_bytes),
        )
        return path

    except Exception as exc:
        # Record failure in manifest if possible
        try:
            path = f"bronze/{source}/year={batch_date.year}/month={batch_date.month:02d}/{batch_id}.parquet"
            await _insert_file_manifest(
                source=source,
                fetch_date=run_date,
                file_path=path,
                sha256_hash=sha256,
                byte_size=len(parquet_bytes) if "parquet_bytes" in dir() else 0,
                batch_id=batch_id,
                status="FAILED",
                conn=conn,
            )
        except Exception:
            pass
        log.error("ingest_failed", source=source, error=str(exc))
        raise

    finally:
        await conn.close()
