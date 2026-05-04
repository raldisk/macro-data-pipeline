"""
Schema hash utility — LOCKED Wave 1 artifact.

Used exclusively by the orchestration agent when writing dataset_versions.schema_hash.
Deterministic: identical schemas always produce the same hash across runs.
No agent reimplements this derivation.
"""
import hashlib
import json

from pyspark.sql import DataFrame


def compute_schema_hash(df: DataFrame) -> str:
    """
    SHA-256 of sorted column-name → DataType string mapping.

    Derivation:
        hashlib.sha256(
            json.dumps(sorted({f.name: str(f.dataType)
                for f in df.schema.fields})).encode()
        ).hexdigest()
    """
    mapping = {f.name: str(f.dataType) for f in df.schema.fields}
    serialized = json.dumps(mapping, sort_keys=True).encode()
    return hashlib.sha256(serialized).hexdigest()
