# pyspark-lakehouse-pipeline

Production-grade PySpark batch pipeline — PSA + BSP macroeconomic data — Bronze → Silver → Gold on S3/MinIO, Prefect-orchestrated, contract-first, wave-executed.

## Two-Repo Architecture

| Concern | This repo | DuckDB/FastAPI repo |
|---|---|---|
| Raw ingestion | YES | NO |
| Bronze/Silver/Gold Parquet | YES | NO |
| PySpark transforms | YES | NO |
| Pipeline metadata (Postgres) | YES — writes | read-only |
| Analytics queries / dashboards | NO | YES |

## Quickstart

```bash
cp .env.example .env
# Pin Prefect: pip index versions prefect → replace 3.x.x in pyproject.toml
make docker-up
make migrate
make run
make backfill START=2023-01 END=2024-12
```

## Wave Model

Wave 1 (contracts) → Wave 2 parallel (INGESTION + TRANSFORM + QUALITY + API) → Wave 3 (ORCHESTRATION) → Wave 4 (INTEGRATION)

**No agent modifies Wave 1 artifacts after lock.**

## API Endpoints

| Endpoint | Description |
|---|---|
| GET /health | Postgres + S3 liveness |
| GET /runs | Pipeline run list |
| GET /runs/{run_id} | Run detail |
| GET /runs/{run_id}/lineage | Run → S3 path mapping |
| GET /datasets | Latest dataset versions |
| GET /datasets/{name}/quality | Quality check results |

## Non-Negotiable Rules

- `src/utils/` frozen after Wave 1
- Ingestion never imports PySpark; Transform never calls boto3
- Postgres stores metadata only — all curated data in S3/MinIO as Parquet
- `batch_date` always day=1 via `normalize_batch_date()`
- `pyproject.toml` frozen after Wave 1
