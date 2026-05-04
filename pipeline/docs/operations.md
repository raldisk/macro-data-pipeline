# Operations

## Running the Pipeline

```bash
make run                              # current month
make backfill START=2023-01 END=2023-12
```

## Checking Health

```bash
curl http://localhost:8000/health
curl http://localhost:8000/runs?limit=5
curl http://localhost:8000/datasets
```

## Viewing Lineage

```bash
curl http://localhost:8000/runs/{run_id}/lineage
```

## Re-running a Failed Batch

```bash
# Bypasses processed_batches guard
make backfill START=2024-03 END=2024-03
```

## Checking Quality Results

```bash
curl http://localhost:8000/datasets/gold_macro_indicators/quality
```
