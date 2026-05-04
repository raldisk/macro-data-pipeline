# Architecture

## Two-Repo Design

This repository (Repo B) handles compute and storage only.
Repo A (DuckDB/FastAPI) handles analytics serving.

## Data Flow

PSA/BSP → Ingestion (pyarrow, boto3) → Bronze (S3)
Bronze → Transform (PySpark) → Silver (S3)
Silver → Quality (PySpark, REGISTRY) → Gold (S3) + Quarantine
Gold → Repo A reads directly via DuckDB

## Wave Gate Model

Wave 1 → freeze all contracts → Wave 2 (4 parallel agents) → Wave 3 orchestration → Wave 4 integration

## Idempotency

- file_manifest: (source, sha256_hash) — content-level dedup
- processed_batches: (source, batch_date) — period-level guard
- partitionOverwriteMode=dynamic: safe re-runs without destroying adjacent partitions
