# ph-macro-lakehouse

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Prefect](https://img.shields.io/badge/Prefect-3.2.1-024DFD?style=for-the-badge&logo=prefect&logoColor=white)](https://www.prefect.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-required-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/YOUR_USERNAME/ph-macro-lakehouse/ci.yml?branch=master&label=CI&style=for-the-badge&logo=github)](https://github.com/YOUR_USERNAME/ph-macro-lakehouse/actions)

> **Philippine Macroeconomic Data Lakehouse** — a production-grade batch pipeline for PSA CPI and BSP FX rates with a live monitoring dashboard.

Data flows from public government sources through **Bronze → Silver → Gold** Parquet layers on S3/MinIO, orchestrated by Prefect, served by FastAPI, and visualised in React.

---

## Table of Contents

- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Service Endpoints](#service-endpoints)
- [Running the Pipeline](#running-the-pipeline)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Data Contracts](#data-contracts)
- [Development](#development)
- [Failure Modes](#failure-modes)
- [Tech Stack](#tech-stack)

---

## Repository Layout

```
ph-macro-lakehouse/
├── docker-compose.yml   ← run everything from here
├── pipeline/            ← Python · PySpark · Prefect · FastAPI
└── dashboard/           ← React · Vite · TypeScript
```

`pipeline/` and `dashboard/` use separate runtimes (Python + Java 17 vs Node 20) and cannot share a package manager. The root `docker-compose.yml` is the single entry point that wires both together.

---

## Architecture

![Architecture Diagram](docs/architecture.svg)

**Key design rules:**
- Postgres stores metadata only — all curated data lives in S3 as Parquet
- Ingestion never imports PySpark; transforms never call boto3 directly
- `partitionOverwriteMode=dynamic` — re-runs overwrite only the target month
- Idempotency via `file_manifest(source, sha256_hash)` and `processed_batches(source, batch_date)`

---

## Quick Start

**Prerequisites:** [Docker 24+](https://docs.docker.com/get-docker/) with Compose v2.

```bash
git clone https://github.com/YOUR_USERNAME/ph-macro-lakehouse.git
cd ph-macro-lakehouse

# Copy env files — defaults work for local MinIO, no edits needed
cp pipeline/.env.example pipeline/.env
cp dashboard/.env.example dashboard/.env

# Start infrastructure + API
docker compose up -d postgres minio minio-init prefect-server prefect-worker api

# Wait for API health (~60s on first run)
until curl -sf http://localhost:8000/health; do echo "waiting..."; sleep 5; done

# Apply database schema
docker compose exec api psql \
  postgresql://lakehouse:lakehouse@postgres:5432/lakehouse \
  -f db/migrations/001_init.sql

# Run the pipeline
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2023-01 --end 2024-12

# Start the dashboard
docker compose up -d dashboard
# → http://localhost:5173
```

> ⚠️ **Known issue affecting dashboard data rendering** (`/gold/*/data` endpoint).
> See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) before starting the dashboard.

---

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:5173 | — |
| API + Swagger | http://localhost:8000/docs | — |
| Prefect UI | http://localhost:4200 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Postgres | `localhost:5432` | `lakehouse` / `lakehouse` |

---

## Running the Pipeline

```bash
# Single month
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2024-04 --end 2024-04

# Multi-month backfill
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2023-01 --end 2024-12

# Verify output
curl http://localhost:8000/runs | python -m json.tool
curl http://localhost:8000/datasets
curl http://localhost:8000/gold/gold_macro_indicators/data

# Check MinIO gold files
docker compose exec minio-init mc ls local/ph-lakehouse/gold/ --recursive
```

**Scheduled runs** — activate the monthly Prefect schedule (1st of each month, 06:00 Asia/Manila):

```bash
docker compose exec api python src/orchestrate/schedules.py
```

---

## API Reference

All endpoints are read-only. `dataset_name` accepts `gold_macro_indicators` or `gold_exchange_rates`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Postgres + S3 liveness |
| `GET` | `/runs` | Run list — `?source=psa\|bsp_fx&limit=N` |
| `GET` | `/runs/{run_id}` | Run detail with stage metrics + quality results |
| `GET` | `/runs/{run_id}/lineage` | Run → S3 path mapping |
| `GET` | `/datasets` | Latest version per gold dataset |
| `GET` | `/datasets/{name}/quality` | Quality check results |
| `GET` | `/gold/{dataset_name}/data` | All rows from latest gold partition |
| `GET` | `/gold/{dataset_name}/latest` | Partition metadata only (no S3 read) |

---

## Configuration

### `pipeline/.env`

```dotenv
S3_ENDPOINT=http://localhost:9000   # empty = real AWS S3
S3_BUCKET=ph-lakehouse
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=ap-southeast-1
DATABASE_URL=postgresql+asyncpg://lakehouse:lakehouse@localhost:5432/lakehouse
PREFECT_API_URL=http://localhost:4200/api
ENV=LOCAL
```

### `dashboard/.env`

```dotenv
VITE_API_URL=    # empty = use Vite proxy (/api → localhost:8000)
```

---

## Data Contracts

Quality rules live in `pipeline/contracts/` and are the single source of truth for the gold layer.

**`gold_macro_indicators.yaml`**
```yaml
partition_key: [period]
schema:
  period: date
  indicator_code: string   # CPI_ALL | CPI_YOY
  value: double
  source: string
quality_checks:
  - { check: no_nulls,      columns: [period, indicator_code, value] }
  - { check: row_count_min, value: 100 }
  - { check: value_range,   column: value, min: -1000000, max: 1000000 }
hard_failure_threshold: 0.05
```

**`gold_exchange_rates.yaml`**
```yaml
partition_key: [period]
schema:
  period: date
  currency_pair: string    # USD/PHP
  rate: double
  source: string
quality_checks:
  - { check: no_nulls,      columns: [period, currency_pair, rate] }
  - { check: row_count_min, value: 20 }
  - { check: value_range,   column: rate, min: 0.0001, max: 1000000 }
hard_failure_threshold: 0.05
```

If failure rate exceeds 5%, the run records `PARTIAL` and rejected rows go to `s3://ph-lakehouse/quarantine/{dataset}/{run_id}/`.

---

## Development

### Backend (no Docker)

```bash
cd pipeline
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d postgres minio minio-init
uvicorn src.api.main:app --reload --port 8000
```

### Frontend (no Docker)

```bash
cd dashboard
npm install
npm run dev   # http://localhost:5173 — proxies /api/* to localhost:8000
```

### Tests

```bash
cd pipeline
pytest tests/unit/ -v             # no infrastructure needed
ENV=TEST pytest tests/ -v         # full suite — needs Postgres + MinIO
```

### Lint

```bash
ruff check src/ tests/ && black --check src/ tests/
```

---

## Failure Modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ValidationError: AWS_ACCESS_KEY_ID missing` | `pipeline/.env` not created | `cp pipeline/.env.example pipeline/.env` |
| `/gold/*/data` returns 503 | Known issue — see [KNOWN_ISSUES.md](KNOWN_ISSUES.md) | Apply patch, rebuild `api` container |
| `UnsupportedClassVersionError` | Java < 17 on native runs | Install Java 17, set `JAVA_HOME` |
| Dashboard chart blank, no errors | Pipeline never run | Run `backfill` command, reload |
| `prefect-worker` connection refused | `prefect-server` still booting (up to 120s) | Wait — worker retries automatically |
| MinIO bucket not found | `minio-init` exited early | `docker compose up minio-init` |
| `HardQualityFailure` | >5% rows failed quality checks | Check `/datasets/{name}/quality`, adjust `contracts/*.yaml`, re-run |

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Orchestration | Prefect 3.2.1 |
| Data transforms | PySpark 3.5.1 · PyArrow 14 |
| Object storage | MinIO · AWS S3 (S3A) |
| API | FastAPI 0.111 · uvicorn · asyncpg |
| Metadata store | PostgreSQL 15 |
| Ingestion | httpx · BeautifulSoup4 · boto3 |
| Dashboard | React 18 · TypeScript 5 · Recharts · Tailwind CSS 3 |
| Build | Vite 5 · Docker Compose v2 |
| CI | GitHub Actions |

---

## License

[MIT](LICENSE)
