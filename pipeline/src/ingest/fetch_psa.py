"""
PSA OpenSTAT fetch — ingestion agent.

Sources:
  CPI All Items (2018=100): DB__2M__PI__CPI__2018/0012M4PCPIAa.px
  CPI YoY inflation (%):    DB__2M__PI__CPI__2018/0022M4PCPIAb.px

Period format: "2024M01" → date(2024, 1, 1)
Timeout: 60s (PSA API is notoriously slow)
Retries: 3, exponential backoff base 2.0
Fallback: data/fixtures/cpi_seed.csv when live API returns empty

Prohibited: importing pyspark, calling SparkStorageClient.
"""
from __future__ import annotations

import csv
import io
import json
import time
from datetime import date
from pathlib import Path

import httpx

from src.utils.logging import get_logger

log = get_logger(__name__)

_BASE_URL    = "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB"
_SERIES: list[tuple[str, str]] = [
    ("CPI_ALL_ITEMS",   "DB__2M__PI__CPI__2018/0012M4PCPIAa.px"),
    ("CPI_YOY_CHANGE",  "DB__2M__PI__CPI__2018/0022M4PCPIAb.px"),
]
_TIMEOUT  = 60.0
_RETRIES  = 3
_BACKOFF  = 2.0
_FIXTURES = Path(__file__).parent.parent.parent / "data" / "fixtures" / "cpi_seed.csv"


def _period_to_date(period: str) -> str:
    """Convert "2024M01" → "2024-01-01"."""
    year, month = period.split("M")
    return f"{year}-{month.zfill(2)}-01"


def _fetch_with_retry(url: str, method: str = "GET", body: dict | None = None) -> dict:
    for attempt in range(_RETRIES):
        try:
            with httpx.Client(timeout=_TIMEOUT) as client:
                if method == "POST":
                    resp = client.post(url, json=body, headers={"Accept": "application/json"})
                else:
                    resp = client.get(url, headers={"Accept": "application/json"})
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            if attempt == _RETRIES - 1:
                raise
            wait = _BACKOFF ** attempt
            log.warning("psa_fetch_retry", attempt=attempt, wait=wait, error=str(exc))
            time.sleep(wait)
    raise RuntimeError("unreachable")


def _fetch_series(series_code: str, table_path: str) -> list[dict]:
    """Fetch one PSA CPI series. Returns list of raw bronze records."""
    meta_url = f"{_BASE_URL}/{table_path}"
    meta     = _fetch_with_retry(meta_url)

    # Discover geo + time dimension codes from metadata
    variables = meta.get("variables", [])
    if len(variables) < 2:
        log.warning("psa_unexpected_metadata", table=table_path)
        return []

    geo_var  = variables[0]
    time_var = variables[-1]

    query_body = {
        "query": [
            {
                "code": geo_var["code"],
                "selection": {
                    "filter": "item",
                    "values": [geo_var["values"][0]],  # national total
                },
            },
            {
                "code": time_var["code"],
                "selection": {
                    "filter": "item",
                    "values": time_var["values"],  # all time periods
                },
            },
        ],
        "response": {"format": "json-stat"},
    }

    data = _fetch_with_retry(meta_url, method="POST", body=query_body)

    # Parse JSON-stat response
    records: list[dict] = []
    dataset = data.get("dataset", data)
    values  = dataset.get("value", [])
    dims    = dataset.get("dimension", {})

    time_dim = list(dims.keys())[-1]
    periods  = list(dims[time_dim]["category"]["label"].values())

    for i, val in enumerate(values):
        period = periods[i] if i < len(periods) else None
        if period and val is not None:
            records.append({
                "series_code": series_code,
                "period":      period,
                "value":       float(val),
                "unit":        "index (2018=100)" if "ALL_ITEMS" in series_code else "percent",
                "source":      "psa_openstat",
            })

    return records


def _load_fixture_fallback() -> list[dict]:
    """Load seed CSV when live API returns empty."""
    if not _FIXTURES.exists():
        log.warning("psa_fixture_not_found", path=str(_FIXTURES))
        return []
    records = []
    with open(_FIXTURES) as f:
        for row in csv.DictReader(f):
            records.append({
                "series_code": row["series_code"],
                "period":      row["period"],
                "value":       float(row["value"]) if row["value"] else None,
                "unit":        row.get("unit", ""),
                "source":      row.get("source", "psa_fixture"),
            })
    log.info("psa_fixture_loaded", rows=len(records))
    return records


def fetch_psa_raw() -> list[dict]:
    """
    Fetch all PSA CPI series.
    Falls back to fixture data if all live series return empty.

    Returns:
        List of raw bronze record dicts.
    """
    all_records: list[dict] = []

    for series_code, table_path in _SERIES:
        try:
            records = _fetch_series(series_code, table_path)
            if records:
                all_records.extend(records)
                log.info("psa_series_fetched", series=series_code, rows=len(records))
            else:
                log.warning("psa_series_empty", series=series_code)
        except Exception as exc:
            log.error("psa_series_failed", series=series_code, error=str(exc))

    if not all_records:
        log.warning("psa_all_series_empty_using_fixture")
        all_records = _load_fixture_fallback()

    return all_records
