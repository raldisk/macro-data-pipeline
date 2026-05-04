# Runbook

## HardQualityFailure

Trigger: failure_rate > hard_failure_threshold (0.05)
Action:
  1. Check quality_results table for failed checks
  2. Inspect quarantine path: s3://ph-lakehouse/quarantine/{dataset}/{run_id}/
  3. Fix upstream source data or adjust threshold in contracts/*.yaml
  4. Re-run: make backfill START=YYYY-MM END=YYYY-MM

## S3 / MinIO Outage

Symptom: GET /health returns {"storage": "error: ..."}
Action:
  1. Verify MinIO is running: docker-compose ps minio
  2. Restart: docker-compose restart minio
  3. Pipeline will retry on next scheduled run (retries=2 per task)

## Postgres Connection Failure

Symptom: GET /health returns {"db": "error: ..."}
Action:
  1. docker-compose restart postgres
  2. Verify migrations: make migrate

## Partition Corruption

Symptom: Silver data missing for a month after re-run
Cause: partitionOverwriteMode not set to dynamic
Fix: Verify get_spark_session() config — must have partitionOverwriteMode=dynamic
Re-run: make backfill START=affected-month END=affected-month

## Bronze Dedup Failure

Symptom: UNIQUE violation on file_manifest insert
Cause: Two processes running ingest for same source simultaneously
Fix: Ensure single Prefect worker; check for duplicate flows in Prefect UI
