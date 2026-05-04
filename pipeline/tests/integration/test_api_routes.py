"""
Integration tests for all six API routes.
Uses FastAPI TestClient against a live test Postgres instance.

Run with:
    ENV=TEST pytest tests/integration/test_api_routes.py -v
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, datetime, timezone
from typing import Generator

import asyncpg
import pytest
from fastapi.testclient import TestClient

_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://lakehouse:lakehouse@localhost:5432/lakehouse_test",
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client() -> Generator[TestClient, None, None]:
    """TestClient wired to test DB. Patches settings for isolation."""
    from unittest.mock import patch
    with patch("src.utils.config.settings") as m:
        m.DATABASE_URL        = _DB_URL
        m.S3_ENDPOINT         = ""
        m.S3_BUCKET           = "ph-lakehouse-test"
        m.AWS_ACCESS_KEY_ID   = "test"
        m.AWS_SECRET_ACCESS_KEY = "test"
        m.AWS_DEFAULT_REGION  = "ap-southeast-1"
        m.ENV                 = "TEST"
        from src.api.main import app
        yield TestClient(app)


@pytest.fixture(scope="module")
def seeded_run_id() -> str:
    """
    Insert one pipeline_run + dataset_version + quality_result row
    so route assertions have data to work against.
    Returns the run_id string.
    """
    async def _seed() -> str:
        sync_url = _DB_URL.replace("postgresql+asyncpg", "postgresql")
        conn = await asyncpg.connect(_DB_URL.replace("postgresql+asyncpg://", "postgresql://"))
        try:
            run_id = await conn.fetchval(
                """
                INSERT INTO pipeline_runs
                    (pipeline_name, source, run_date, status,
                     records_ingested, records_rejected)
                VALUES ('ph_lakehouse_pipeline', 'psa', $1, 'SUCCESS', 1000, 5)
                RETURNING run_id::text
                """,
                date.today(),
            )
            schema_hash = "abc123def456"
            s3_path = f"s3a://ph-lakehouse-test/gold/gold_macro_indicators/year=2024/month=01/"

            await conn.execute(
                """
                INSERT INTO dataset_versions
                    (run_id, dataset_name, partition_key, row_count, schema_hash, s3_path)
                VALUES ($1::uuid, $2, $3, $4, $5, $6)
                """,
                run_id, "gold_macro_indicators", "year=2024/month=01", 995, schema_hash, s3_path,
            )
            await conn.execute(
                """
                INSERT INTO quality_results (run_id, check_name, passed, failed_count, threshold)
                VALUES ($1::uuid, 'no_nulls', true, 0, 0.05),
                       ($1::uuid, 'row_count_min', true, 0, 0.05),
                       ($1::uuid, 'value_range', true, 5, 0.05)
                """,
                run_id,
            )
            await conn.execute(
                """
                INSERT INTO processed_batches (source, batch_date, run_id)
                VALUES ('psa', $1, $2::uuid)
                ON CONFLICT DO NOTHING
                """,
                date(2024, 1, 1), run_id,
            )
            return run_id
        finally:
            await conn.close()

    return asyncio.get_event_loop().run_until_complete(_seed())


# ── /health ───────────────────────────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_health_returns_status(client):
    resp = client.get("/health")
    assert resp.status_code in (200, 503)
    body = resp.json()
    assert "status" in body
    assert "db" in body
    assert "storage" in body


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_health_db_field_is_string(client):
    resp = client.get("/health")
    assert isinstance(resp.json()["db"], str)


# ── GET /runs ─────────────────────────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_runs_returns_list(client, seeded_run_id):
    resp = client.get("/runs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) >= 1


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_runs_source_filter(client, seeded_run_id):
    resp = client.get("/runs?source=psa")
    assert resp.status_code == 200
    for run in resp.json():
        assert run["source"] == "psa"


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_runs_limit_respected(client, seeded_run_id):
    resp = client.get("/runs?limit=1")
    assert resp.status_code == 200
    assert len(resp.json()) <= 1


# ── GET /runs/{run_id} ────────────────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_run_detail_fields(client, seeded_run_id):
    resp = client.get(f"/runs/{seeded_run_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == seeded_run_id
    assert body["source"] == "psa"
    assert body["status"] == "SUCCESS"
    assert "stage_metrics" in body
    assert "quality_results" in body


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_run_quality_results_present(client, seeded_run_id):
    resp = client.get(f"/runs/{seeded_run_id}")
    qr = resp.json()["quality_results"]
    assert len(qr) == 3
    check_names = {r["check_name"] for r in qr}
    assert "no_nulls" in check_names
    assert "row_count_min" in check_names


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_run_not_found_404(client):
    fake = "00000000-0000-0000-0000-000000000000"
    assert client.get(f"/runs/{fake}").status_code == 404


# ── GET /runs/{run_id}/lineage ────────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_lineage_returns_s3_path(client, seeded_run_id):
    resp = client.get(f"/runs/{seeded_run_id}/lineage")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) >= 1
    assert "s3_path" in rows[0]
    assert rows[0]["s3_path"].startswith("s3a://")


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_lineage_contains_dataset_name(client, seeded_run_id):
    rows = client.get(f"/runs/{seeded_run_id}/lineage").json()
    assert rows[0]["dataset_name"] == "gold_macro_indicators"


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_lineage_not_found_404(client):
    fake = "00000000-0000-0000-0000-000000000000"
    assert client.get(f"/runs/{fake}/lineage").status_code == 404


# ── GET /datasets ─────────────────────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_datasets_returns_list(client, seeded_run_id):
    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    names = {d["dataset_name"] for d in resp.json()}
    assert "gold_macro_indicators" in names


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_datasets_has_schema_hash(client, seeded_run_id):
    resp = client.get("/datasets")
    entry = next(d for d in resp.json() if d["dataset_name"] == "gold_macro_indicators")
    assert entry["schema_hash"] == "abc123def456"
    assert entry["row_count"] == 995


# ── GET /datasets/{name}/quality ──────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_dataset_quality_returns_checks(client, seeded_run_id):
    resp = client.get("/datasets/gold_macro_indicators/quality")
    assert resp.status_code == 200
    checks = resp.json()
    assert len(checks) >= 1
    assert all("check_name" in c and "passed" in c for c in checks)


@pytest.mark.skipif(os.environ.get("ENV") != "TEST", reason="Requires TEST Postgres")
def test_dataset_quality_unknown_name_404(client):
    assert client.get("/datasets/nonexistent_dataset/quality").status_code == 404
