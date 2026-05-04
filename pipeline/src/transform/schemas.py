"""
Schema definitions — LOCKED Wave 1 artifact.

THIS FILE IS THE SOLE SOURCE OF TRUTH for all bronze and silver StructType definitions.
contracts/*.yaml is the sole source of truth for gold schema.
These domains do not overlap — no column defined here appears in a YAML, and vice versa.

Field definitions are anchored to Appendix A of the spec (production-verified).
No agent invents column names outside this file.
"""
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── PSA: Consumer Price Index ─────────────────────────────────────────────────

PSA_BRONZE_SCHEMA = StructType([
    StructField("series_code", StringType(),    nullable=False),  # CPI_ALL_ITEMS | CPI_YOY_CHANGE
    StructField("period",      StringType(),    nullable=False),  # raw PSA label e.g. "2024M01"
    StructField("value",       DoubleType(),    nullable=True),
    StructField("unit",        StringType(),    nullable=True),
    StructField("source",      StringType(),    nullable=False),
    StructField("fetched_at",  TimestampType(), nullable=False),
    StructField("batch_id",    StringType(),    nullable=False),
])

PSA_SILVER_SCHEMA = StructType([
    StructField("period_date",  DateType(),    nullable=False),  # converted from "2024M01"
    StructField("period_year",  IntegerType(), nullable=False),
    StructField("period_month", IntegerType(), nullable=False),
    StructField("series_code",  StringType(),  nullable=False),
    StructField("value",        DoubleType(),  nullable=True),
    StructField("unit",         StringType(),  nullable=True),
    StructField("source",       StringType(),  nullable=False),
])

# Dedup natural key: (series_code, period_date)

# ── BSP: Exchange Rates (Table 12 — USD/PHP monthly) ─────────────────────────

BSP_FX_BRONZE_SCHEMA = StructType([
    StructField("rate_date",     StringType(),    nullable=False),  # ISO date string
    StructField("currency_pair", StringType(),    nullable=False),
    StructField("rate",          DoubleType(),    nullable=True),
    StructField("source",        StringType(),    nullable=False),
    StructField("fetched_at",    TimestampType(), nullable=False),
    StructField("batch_id",      StringType(),    nullable=False),
])

BSP_FX_SILVER_SCHEMA = StructType([
    StructField("period_date",   DateType(),    nullable=False),  # cast from rate_date
    StructField("currency_pair", StringType(),  nullable=False),
    StructField("rate",          DoubleType(),  nullable=False),
    StructField("source",        StringType(),  nullable=False),
])

# Dedup natural key: (currency_pair, period_date)

# ── BSP: Policy Rate ──────────────────────────────────────────────────────────

BSP_POLICY_SILVER_SCHEMA = StructType([
    StructField("decision_date", DateType(),    nullable=False),
    StructField("overnight_rp",  DoubleType(),  nullable=False),
    StructField("overnight_srp", DoubleType(),  nullable=True),   # nullable
    StructField("direction",     StringType(),  nullable=False),   # hike | cut | hold
    StructField("source",        StringType(),  nullable=False),
])

# Sort requirement: always ascending by decision_date after parse.
# BSP HTML tables are descending — silver write must preserve ascending order.

# ── Helpers ───────────────────────────────────────────────────────────────────

_SCHEMA_MAP = {
    "psa_bronze":    PSA_BRONZE_SCHEMA,
    "psa_silver":    PSA_SILVER_SCHEMA,
    "bsp_fx_bronze": BSP_FX_BRONZE_SCHEMA,
    "bsp_fx_silver": BSP_FX_SILVER_SCHEMA,
    "bsp_policy_silver": BSP_POLICY_SILVER_SCHEMA,
}


def get_bronze_schema(source: str) -> StructType:
    key = f"{source}_bronze"
    if key not in _SCHEMA_MAP:
        raise KeyError(f"No bronze schema registered for source '{source}'")
    return _SCHEMA_MAP[key]


def get_silver_schema(source: str) -> StructType:
    key = f"{source}_silver"
    if key not in _SCHEMA_MAP:
        raise KeyError(f"No silver schema registered for source '{source}'")
    return _SCHEMA_MAP[key]
