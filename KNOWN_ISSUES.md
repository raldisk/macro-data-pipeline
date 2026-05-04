# Known Issues

---

## [KI-001] Gold serving layer — S3 path scheme and partitioned dataset

**Status:** Open — manual patch required
**Affects:** `pipeline/src/api/routes/gold.py`
**Symptom:** `GET /gold/{dataset_name}/data` returns 503
**Fix Target:** v0.2.0

---

### Root Cause

Two assumptions in `gold.py` do not match actual pipeline output:

1. `_parse_s3_path` accepts only `s3://` prefixes, but `process_quality_and_write_gold` writes `s3a://` paths to `dataset_versions.s3_path`.
2. `_read_parquet_from_s3` calls `s3.get_object` on a single key, but the pipeline writes a partitioned directory containing multiple `part-*.parquet` files.

Both failures are silent at write time and only surface when the serving layer attempts to read.

---

### Patch

Open `pipeline/src/api/routes/gold.py` and replace the two functions below.

**Fix `_parse_s3_path` — accept both `s3://` and `s3a://`:**

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

**Fix `_read_parquet_from_s3` — list and read all part files under the prefix:**

```python
def _read_parquet_from_s3(s3_path: str) -> list[dict[str, Any]]:
    bucket, prefix = _parse_s3_path(s3_path)
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not keys:
        raise FileNotFoundError(f"No Parquet files under: s3://{bucket}/{prefix}")
    rows: list[dict] = []
    for key in keys:
        buf = io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        for batch in pq.read_table(buf).to_batches():
            for i in range(batch.num_rows):
                row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
                rows.append({k: v.isoformat() if hasattr(v, "isoformat") else v
                             for k, v in row.items()})
    return rows
```

---

### Apply

After saving the file, rebuild the API container:

```bash
docker compose up -d --build api
```

Confirm the fix:

```bash
curl http://localhost:8000/gold/gold_macro_indicators/data
# Expected: {"dataset":"gold_macro_indicators","rows":[...],"count":N}
```
