-- 001_init.sql — full schema for PH Lakehouse metadata store
-- Postgres stores pipeline metadata only — never curated data.
-- All curated data lives in S3/MinIO as Parquet.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── OWNER: orchestration agent ─────────────────────────────────────────────

CREATE TABLE pipeline_runs (
    run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name    TEXT NOT NULL,
    source           TEXT NOT NULL,
    run_date         DATE NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at         TIMESTAMPTZ,
    status           TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    records_ingested INT,
    records_rejected INT,
    error_message    TEXT
);

CREATE TABLE stage_metrics (
    id               SERIAL PRIMARY KEY,
    run_id           UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    stage_name       TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    duration_seconds FLOAT,
    input_rows       INT,
    output_rows      INT
);

CREATE TABLE dataset_versions (
    id               SERIAL PRIMARY KEY,
    run_id           UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    dataset_name     TEXT NOT NULL,
    partition_key    TEXT NOT NULL,
    row_count        INT NOT NULL,
    schema_hash      TEXT NOT NULL,
    -- schema_hash derivation (src/utils/schema.py):
    --   hashlib.sha256(
    --     json.dumps(sorted({f.name: str(f.dataType)
    --       for f in df.schema.fields})).encode()
    --   ).hexdigest()
    s3_path          TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE processed_batches (
    source           TEXT NOT NULL,
    batch_date       DATE NOT NULL,
    -- batch_date is ALWAYS the first day of the month (normalize_batch_date()).
    -- A run on 2023-01-15 → batch_date = 2023-01-01.
    -- Enforced at write time via normalize_batch_date(). Never derived ad hoc.
    run_id           UUID REFERENCES pipeline_runs(run_id),
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, batch_date)
);

-- ── OWNER: quality agent (writes); orchestration (reads for commit) ────────

CREATE TABLE quality_results (
    id               SERIAL PRIMARY KEY,
    run_id           UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    check_name       TEXT NOT NULL,
    passed           BOOLEAN NOT NULL,
    failed_count     INT DEFAULT 0,
    threshold        FLOAT
);

-- ── OWNER: ingestion agent ─────────────────────────────────────────────────

CREATE TABLE file_manifest (
    id               SERIAL PRIMARY KEY,
    source           TEXT NOT NULL,
    fetch_date       DATE NOT NULL,
    file_path        TEXT NOT NULL,
    sha256_hash      TEXT NOT NULL,
    byte_size        BIGINT,
    batch_id         TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('SUCCESS','FAILED','SKIPPED')),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, sha256_hash)
    -- Idempotency key: same content hash = skip, regardless of fetch_date.
    -- Prevents re-download when source data has not changed between runs.
);

-- ── Indexes for API query performance ─────────────────────────────────────

CREATE INDEX idx_pipeline_runs_source       ON pipeline_runs (source);
CREATE INDEX idx_pipeline_runs_status       ON pipeline_runs (status);
CREATE INDEX idx_pipeline_runs_run_date     ON pipeline_runs (run_date);
CREATE INDEX idx_dataset_versions_name      ON dataset_versions (dataset_name);
CREATE INDEX idx_quality_results_run_id     ON quality_results (run_id);
CREATE INDEX idx_file_manifest_source_hash  ON file_manifest (source, sha256_hash);
