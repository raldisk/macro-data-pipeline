# ph-macro-lakehouse

Philippine Macroeconomic Data Lakehouse — a production-grade batch data pipeline for PSA CPI and BSP FX rates with a real-time monitoring dashboard. Built with PySpark, Prefect 3, FastAPI, React, and MinIO.

Data flows from public government sources through Bronze → Silver → Gold Parquet layers stored on S3-compatible object storage. A FastAPI metadata layer surfaces pipeline run history, lineage, and quality check results. A React dashboard visualises monthly CPI and USD/PHP trends directly from the gold layer.

---

## Repository Structure

The repository is deliberately split into two subdirectories because they are genuinely different runtimes. `pipeline/` runs Python 3.11 with Java 17 (required for PySpark), managed by `pip` and `pyproject.toml`. `dashboard/` runs Node 20, managed by `npm` and `package.json`. They cannot share a package manager without breaking both. The root `docker-compose.yml` is the single entry point that wires them together at runtime.

```
ph-macro-lakehouse/
├── docker-compose.yml          # unified compose — all six services

├── pipeline/                   # Python / PySpark / Prefect / FastAPI
│   ├── src/
│   │   ├── api/                # FastAPI — metadata + gold serving
│   │   │   └── routes/
│   │   │       ├── health.py
│   │   │       ├── runs.py
│   │   │       ├── datasets.py
│   │   │       └── gold.py     # ← serving layer: reads Parquet from S3
│   │   ├── ingest/             # PSA + BSP HTTP fetch → bronze Parquet
│   │   ├── transform/          # PySpark silver + gold transforms
│   │   │   ├── quality.py      # contract-driven quality engine
│   │   │   └── spark_jobs/     # silver_psa.py, silver_bsp.py
│   │   ├── orchestrate/        # Prefect flow, tasks, CLI, scheduler
│   │   ├── contracts/          # YAML contract loader
│   │   ├── metadata/           # asyncpg repository — Postgres only
│   │   └── utils/              # spark factory, storage, config, batch
│   ├── contracts/              # gold_macro_indicators.yaml, gold_exchange_rates.yaml
│   ├── db/migrations/          # 001_init.sql — six tables
│   ├── data/fixtures/          # cpi_seed.csv, fx_seed.csv (fallback data)
│   ├── docs/                   # architecture.md, runbook.md, operations.md
│   ├── tests/                  # unit / contract / integration / spark
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── .env.example
└── dashboard/                  # React / Vite / TypeScript / Tailwind / Recharts
    ├── src/
    │   ├── api/client.ts       # typed fetch wrapper — all API contracts
    │   └── App.tsx             # charts, run table, infra panel, quality panel
    ├── vite.config.ts          # /api/* proxy → localhost:8000
    ├── package.json
    └── .env.example
```

---

## What the Dashboard Shows

The top bar displays live Postgres and S3 health indicators pulled from `GET /health`. The metrics strip shows the latest run status, total records ingested, quality pass rate, and next scheduled run time. The left column contains a pipeline runs table — each row is clickable and expands to show per-stage duration, input rows, and output rows — and below it a line chart of monthly gold-layer data with CPI_ALL and USD/PHP on the left Y-axis and CPI_YOY on the right. The right column contains an infrastructure health panel, a dataset versions panel showing partition key, row count, schema hash, and S3 path, and a quality checks panel for `gold_macro_indicators`.

---

## Architecture

Data originates from two Philippine government sources. PSA (Philippine Statistics Authority) publishes CPI data via a public API. BSP (Bangko Sentral ng Pilipinas) publishes USD/PHP exchange rates via HTML table scraping. Neither source is real-time; both publish monthly.

```
PSA API + BSP HTML
       │
       ▼
  Ingestion Layer (boto3, httpx)
  • SHA-256 dedup via file_manifest
  • Writes raw bytes to Bronze (S3/MinIO)
       │
       ▼
  Transform Layer (PySpark 3.5)
  • Type casting, date normalisation, dedup
  • Partitioned Silver Parquet (year/month)
       │
       ▼
  Quality Layer (PySpark + contract YAML)
  • no_nulls, row_count_min, value_range checks
  • Clean rows → Gold Parquet
  • Rejected rows → Quarantine path
       │
       ▼
  Metadata Layer (asyncpg → Postgres)
  • pipeline_runs, stage_metrics, dataset_versions
  • quality_results, processed_batches, file_manifest
       │
       ▼
  Serving Layer (FastAPI + boto3 + PyArrow)
  • Reads gold Parquet from S3 into memory
  • Returns JSON to the dashboard
```

Postgres stores metadata only — it never holds curated data. All curated data lives in S3/MinIO as Parquet. Ingestion never imports PySpark; transforms never call boto3 directly. This boundary is enforced by the `RawStorageClient` / `SparkStorageClient` protocol split in `src/utils/storage.py`.

Idempotency is guaranteed at two levels: `file_manifest(source, sha256_hash)` prevents re-downloading unchanged source files, and `processed_batches(source, batch_date)` prevents re-running a period that has already completed. Spark uses `partitionOverwriteMode=dynamic` so a re-run overwrites only the target month's partition, leaving all other partitions intact.

---

## API Reference

All endpoints are read-only. The API serves metadata from Postgres and gold data from S3. It does not expose Spark sessions or internal pipeline state.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Postgres + S3 liveness. Returns `{"status","db","storage"}`. |
| GET | `/runs` | Pipeline run list. Query params: `?source=psa\|bsp_fx`, `?limit=N`. |
| GET | `/runs/{run_id}` | Run detail including stage metrics and quality results. |
| GET | `/runs/{run_id}/lineage` | Run → S3 path mapping for each dataset version written. |
| GET | `/datasets` | Latest dataset version for each gold dataset. |
| GET | `/datasets/{name}/quality` | Quality check results for the named dataset. |
| GET | `/gold/{dataset_name}/data` | All rows from the latest gold partition as JSON. |
| GET | `/gold/{dataset_name}/latest` | Partition metadata only — no S3 read. |

Valid `dataset_name` values are `gold_macro_indicators` and `gold_exchange_rates`.

---

## Prerequisites

To run everything via Docker Compose (recommended), you only need Docker. To run natively without Docker, you additionally need Python 3.11+, Java 17, and Node 20.

**Verify Docker:**
```bash
docker --version          # need 24+
docker compose version    # need v2 — the plugin, not docker-compose v1
```

**Verify native prerequisites (only if not using Docker):**
```bash
python --version    # 3.11.x or 3.12.x
java -version       # 17.x — Spark 3.5.1 requires Java 17
node --version      # 20.x
npm --version       # ships with Node
```

Java is the single most commonly missing dependency. If absent:

```bash
# macOS
brew install openjdk@17
echo 'export JAVA_HOME=$(brew --prefix openjdk@17)' >> ~/.zshrc && source ~/.zshrc

# Ubuntu / Debian
sudo apt-get install -y openjdk-17-jre-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## Quick Start (Docker Compose — all layers)

```bash
# 1. Clone and enter the repo root
git clone https://github.com/your-username/ph-macro-lakehouse.git
cd ph-macro-lakehouse

# 2. Create pipeline env — defaults work for local MinIO, no edits needed
cp pipeline/.env.example pipeline/.env

# 3. Create dashboard env — empty VITE_API_URL activates the Vite proxy
cp dashboard/.env.example dashboard/.env

# 4. Start infrastructure and API (dashboard added after data exists)
docker compose up -d postgres minio minio-init prefect-server prefect-worker api

# 5. Wait for API to become healthy (~60s on first run while images pull)
until curl -sf http://localhost:8000/health; do echo "waiting..."; sleep 5; done

# 6. Apply the database schema
docker compose exec api psql \
  postgresql://lakehouse:lakehouse@postgres:5432/lakehouse \
  -f db/migrations/001_init.sql

# 7. Run the pipeline for your chosen date range
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2023-01 --end 2024-12

# 8. Verify data exists
curl http://localhost:8000/runs
curl http://localhost:8000/datasets
curl http://localhost:8000/gold/gold_macro_indicators/data

# 9. Start the dashboard
docker compose up -d dashboard

# Dashboard available at http://localhost:5173
```

---

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| FastAPI (metadata + gold) | http://localhost:8000 | — |
| FastAPI interactive docs | http://localhost:8000/docs | — |
| React dashboard | http://localhost:5173 | — |
| Prefect UI | http://localhost:4200 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Postgres | localhost:5432 | lakehouse / lakehouse / lakehouse |

---

## Configuration

### Pipeline (`pipeline/.env`)

These values match the local MinIO defaults and work without modification:

```dotenv
# Object storage — MinIO local (leave S3_ENDPOINT empty for real AWS S3)
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=ph-lakehouse
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=ap-southeast-1

# Postgres
DATABASE_URL=postgresql+asyncpg://lakehouse:lakehouse@localhost:5432/lakehouse

# Prefect
PREFECT_API_URL=http://localhost:4200/api

# Environment
ENV=LOCAL
```

For real AWS S3, set `S3_ENDPOINT=` (empty string), provide real AWS credentials, and update `S3_BUCKET` to your bucket name.

### Dashboard (`dashboard/.env`)

```dotenv
# Leave empty to use the Vite dev proxy (/api → localhost:8000).
# Set to a full URL in production: https://api.your-domain.com
VITE_API_URL=
```

---

## Critical Patch: Gold Serving Layer

> **This patch is required before the dashboard chart will show data.** It is a one-time edit to a single file.

`pipeline/src/api/routes/gold.py` was generated with two assumptions that need correction against the actual pipeline behaviour.

**Fix 1 — S3 path scheme.** The pipeline writes `s3a://` paths to `dataset_versions.s3_path`, but `gold.py` only accepted `s3://`. Open `pipeline/src/api/routes/gold.py` and replace the `_parse_s3_path` function:

```python
def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    for prefix in ("s3a://", "s3://"):
        if s3_path.startswith(prefix):
            without_scheme = s3_path[len(prefix):]
            break
    else:
        raise ValueError(f"Expected s3:// or s3a:// path, got: {s3_path!r}")
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Cannot parse bucket/key from: {s3_path!r}")
    return bucket, key
```

**Fix 2 — Partitioned dataset.** `process_quality_and_write_gold` writes a partitioned directory containing multiple `part-*.parquet` files, not a single file. Replace `_read_parquet_from_s3` with this version that lists and reads all part files:

```python
def _read_parquet_from_s3(s3_path: str) -> list[dict[str, Any]]:
    bucket, prefix = _parse_s3_path(s3_path)
    s3 = _s3_client()

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not keys:
        raise FileNotFoundError(f"No Parquet files found under: s3://{bucket}/{prefix}")

    rows: list[dict] = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        for batch in table.to_batches():
            for i in range(batch.num_rows):
                row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                rows.append(row)

    return rows
```

After saving, rebuild the API container:

```bash
docker compose up -d --build api
```

Confirm the fix worked:

```bash
curl http://localhost:8000/gold/gold_macro_indicators/data
# Expected: {"dataset":"gold_macro_indicators","rows":[...],"count":N}
```

---

## Running the Pipeline

### Single month

```bash
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2024-04 --end 2024-04
```

### Multi-month backfill

```bash
docker compose exec api python -m src.orchestrate.cli backfill \
  --start 2023-01 --end 2024-12
```

The backfill CLI bypasses the `processed_batches` idempotency guard, so re-running a month overwrites that month's partition without affecting any other partition. This is safe because Spark is configured with `partitionOverwriteMode=dynamic`.

### Verify pipeline output

```bash
# Check run status in Postgres via the API
curl http://localhost:8000/runs | python -m json.tool

# Check MinIO has gold files
docker compose exec minio-init mc ls local/ph-lakehouse/gold/ --recursive

# Confirm quality results
curl http://localhost:8000/datasets/gold_macro_indicators/quality | python -m json.tool
```

### Scheduled monthly runs (Prefect)

The pipeline is scheduled to run on the 1st of each month at 06:00 Asia/Manila (UTC+8). To activate the schedule:

```bash
docker compose exec api python src/orchestrate/schedules.py
```

The Prefect UI at http://localhost:4200 shows deployment status and run history.

---

## Development Workflow

### Backend only (no Docker)

```bash
cd pipeline
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# Start Postgres and MinIO via Docker, then run the API natively
docker compose up -d postgres minio minio-init
uvicorn src.api.main:app --reload --port 8000
```

### Frontend only (no Docker)

The Vite proxy in `vite.config.ts` routes `/api/*` to `http://localhost:8000`, so the dashboard talks to a locally running API without any configuration change.

```bash
cd dashboard
npm install
npm run dev
# Opens at http://localhost:5173
```

### Running tests

```bash
cd pipeline
pip install -e ".[dev]"

# Unit tests only (no infrastructure needed)
pytest tests/unit/ -v

# All tests (requires running Postgres and MinIO)
ENV=TEST DATABASE_URL=postgresql+asyncpg://lakehouse:lakehouse@localhost:5432/lakehouse_test \
pytest tests/ -v
```

### Linting and formatting

```bash
cd pipeline
ruff check src/ tests/
black --check src/ tests/

# Auto-fix
black src/ tests/
ruff check --fix src/ tests/
```

---

## Data Contracts

Gold layer schema and quality rules are defined in `pipeline/contracts/`. These YAML files are the source of truth for what the gold layer must contain.

**`gold_macro_indicators.yaml`** — PSA CPI data:

```yaml
dataset: gold_macro_indicators
partition_key: [period]
schema:
  period: date
  indicator_code: string    # CPI_ALL | CPI_YOY
  value: double
  source: string
quality_checks:
  - check: no_nulls
    columns: [period, indicator_code, value]
  - check: row_count_min
    value: 100
  - check: value_range
    column: value
    min: -1000000
    max: 1000000
hard_failure_threshold: 0.05
```

**`gold_exchange_rates.yaml`** — BSP USD/PHP rates:

```yaml
dataset: gold_exchange_rates
partition_key: [period]
schema:
  period: date
  currency_pair: string     # USD/PHP
  rate: double
  source: string
quality_checks:
  - check: no_nulls
    columns: [period, currency_pair, rate]
  - check: row_count_min
    value: 20
  - check: value_range
    column: rate
    min: 0.0001
    max: 1000000
hard_failure_threshold: 0.05
```

If more than 5% of rows in a partition fail quality checks, the pipeline raises `HardQualityFailure` and writes nothing to gold — the run records `PARTIAL` status and all rejected rows go to `s3://ph-lakehouse/quarantine/{dataset}/{run_id}/`.

---

## Database Schema

Postgres holds six tables. None of them store curated economic data — that lives in S3 as Parquet.

`pipeline_runs` tracks every pipeline execution with status, timestamps, and record counts. `stage_metrics` records duration and row counts per pipeline stage within a run. `dataset_versions` records the S3 path, row count, and schema hash for each gold partition written. `processed_batches` is the idempotency guard — one row per (source, batch_date) pair prevents re-processing an already-completed month. `quality_results` stores the pass/fail outcome of each quality check per run. `file_manifest` deduplicates raw source files by SHA-256 hash so unchanged source data is never re-downloaded.

Apply the schema:

```bash
docker compose exec api psql \
  postgresql://lakehouse:lakehouse@postgres:5432/lakehouse \
  -f db/migrations/001_init.sql
```

---

## Common Failure Modes and Fixes

### API container exits immediately: `ValidationError: AWS_ACCESS_KEY_ID missing`

`pipeline/.env` does not exist. Run `cp pipeline/.env.example pipeline/.env` and restart: `docker compose up -d api`.

### Gold endpoint returns 503: `"Gold file unreachable"`

Either the critical patch in the Gold Serving Layer section above was not applied, or the `api` container was not rebuilt after the patch. Apply the patch, then run `docker compose up -d --build api`.

### Spark task fails with `java.lang.UnsupportedClassVersionError`

This only affects native (non-Docker) runs. Your system Java is below version 17. Confirm with `java -version` and follow the Java 17 installation steps in Prerequisites.

### Dashboard shows blank chart with no errors in the browser

The pipeline has not run yet for any date range, so there are no gold files in MinIO and no rows in `dataset_versions`. Run the backfill command from the Running the Pipeline section, then reload the browser.

### `prefect-worker` fails to connect after startup

`prefect-server` takes up to 120 seconds to complete its database migration on first boot. The worker depends on `service_healthy` and retries automatically. Run `docker compose ps` — once `prefect-server` shows `healthy`, the worker connects within 30 seconds.

### MinIO bucket not found

`minio-init` is a one-shot container that creates the bucket on first start. If it exited before MinIO was healthy, re-run it: `docker compose up minio-init`. Confirm the bucket exists with `docker compose exec minio-init mc ls local/`.

### `HardQualityFailure` during pipeline run

More than 5% of rows in a partition failed quality checks. Check which checks failed:

```bash
curl http://localhost:8000/datasets/gold_macro_indicators/quality
```

Rejected rows are available for inspection at `s3://ph-lakehouse/quarantine/{dataset}/{run_id}/`. Adjust the thresholds in `contracts/*.yaml` if the source data legitimately changed shape, then re-run:

```bash
docker compose exec api python -m src.orchestrate.cli backfill \
  --start YYYY-MM --end YYYY-MM
```

### CORS errors in the browser console

Confirm the dashboard is loading from `http://localhost:5173` and not from a `file://` URL. The Vite dev proxy only operates when the page is served by Vite's dev server. FastAPI has `allow_origins=["*"]` for GET requests, so direct API calls also work — but the Vite proxy is the intended path for development.

---

## Production Notes

The `docker-compose.yml` dashboard service runs Vite's dev server, which is appropriate for evaluation and development but not for production. For production, build the dashboard as a static site and serve it via nginx:

```bash
cd dashboard
npm run build
# Serve dist/ via nginx or any static host
```

The `pipeline/src/api/routes/gold.py` serving layer reads entire Parquet partitions into memory on each request. For datasets with millions of rows, add pagination parameters (`?limit=N&offset=N`) or pre-aggregate the gold layer to a summary table. The current implementation is correct for monthly macroeconomic data where full-partition loads are small.

Prefect version pins (`prefect==3.2.1`, `prefect-aws==0.5.0`) in `pyproject.toml` should be re-verified against the PyPI release index before a production deploy and advanced to the current stable patch if newer.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Batch orchestration | Prefect 3.2.1 |
| Data transforms | PySpark 3.5.1 |
| Object storage | MinIO (S3-compatible) / AWS S3 |
| Serialisation | Apache Parquet via PyArrow 14 |
| API framework | FastAPI 0.111 + uvicorn |
| Async DB client | asyncpg 0.29 |
| Metadata store | PostgreSQL 15 |
| HTTP client | httpx 0.27 |
| HTML scraping | BeautifulSoup4 4.12 |
| Frontend framework | React 18 + TypeScript 5 |
| Build tool | Vite 5 |
| Charting | Recharts 2.12 |
| Styling | Tailwind CSS 3.4 |
| Containerisation | Docker Compose v2 |

---

## License

MIT
