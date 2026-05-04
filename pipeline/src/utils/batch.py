"""
Batch identity — LOCKED Wave 1 artifact.

Both functions are used in:
  bronze path construction, file_manifest inserts,
  processed_batches inserts, orchestration.

No agent derives batch_id or batch_date independently.
"""
from datetime import date
from uuid import uuid4


def make_batch_id() -> str:
    """Generate a new UUID4 batch identifier."""
    return str(uuid4())


def normalize_batch_date(run_date: date) -> date:
    """
    Always returns the first day of the month.

    Rationale: S3 partitions use year=YYYY/month=MM keys.
    Without this normalization, (psa, 2023-01-01) and (psa, 2023-01-15)
    would produce separate rows in processed_batches, breaking idempotency.

    Examples:
        normalize_batch_date(date(2023, 1, 15)) == date(2023, 1, 1)
        normalize_batch_date(date(2023, 1,  1)) == date(2023, 1, 1)
    """
    return run_date.replace(day=1)
