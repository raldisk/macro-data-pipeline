from src.transform.schemas import (
    PSA_BRONZE_SCHEMA, PSA_SILVER_SCHEMA,
    BSP_FX_SILVER_SCHEMA, BSP_POLICY_SILVER_SCHEMA,
    get_bronze_schema, get_silver_schema,
)
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType
import pytest

def _field(schema, name):
    fields = {f.name: f for f in schema.fields}
    assert name in fields
    return fields[name]

def test_psa_bronze_series_code_not_nullable():
    f = _field(PSA_BRONZE_SCHEMA, "series_code")
    assert not f.nullable

def test_psa_silver_period_date_is_date():
    f = _field(PSA_SILVER_SCHEMA, "period_date")
    assert isinstance(f.dataType, DateType)
    assert not f.nullable

def test_bsp_fx_silver_rate_not_nullable():
    f = _field(BSP_FX_SILVER_SCHEMA, "rate")
    assert not f.nullable
    assert isinstance(f.dataType, DoubleType)

def test_bsp_policy_overnight_srp_nullable():
    f = _field(BSP_POLICY_SILVER_SCHEMA, "overnight_srp")
    assert f.nullable

def test_get_bronze_schema_psa():
    assert get_bronze_schema("psa") == PSA_BRONZE_SCHEMA

def test_get_silver_schema_psa():
    assert get_silver_schema("psa") == PSA_SILVER_SCHEMA

def test_get_schema_unknown_raises():
    with pytest.raises(KeyError):
        get_bronze_schema("nonexistent")
