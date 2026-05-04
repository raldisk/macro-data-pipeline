"""
BSP fetch — ingestion agent.

Sources:
  Table 12: USD/PHP monthly average — HTML scrape
  Policy Rate: overnight_rp — HTML scrape

Timeout: 30s
Retries: 3, exponential backoff
User-Agent: Mozilla/5.0 (compatible; ph-dashboard/1.0)

Table 12 column layout:
  col 0  = Year (int)
  cols 1–12 = monthly averages Jan–Dec (float)
  missing values: "-" or ".." → None

BSP Policy Rate validation:
  overnight_rp must be 0.5–20.0
  direction: hike | cut | hold
  decision_date must not be in the future

Prohibited: importing pyspark, calling SparkStorageClient.
"""
from __future__ import annotations

import csv
import time
from datetime import date, datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from src.utils.logging import get_logger

log = get_logger(__name__)

_TABLE12_URL    = "https://www.bsp.gov.ph/statistics/external/tab12_pus_data.aspx"
_POLICY_URL     = "https://www.bsp.gov.ph/monetary_policy/key_rates.aspx"
_TIMEOUT        = 30.0
_RETRIES        = 3
_BACKOFF        = 2.0
_HEADERS        = {"User-Agent": "Mozilla/5.0 (compatible; ph-dashboard/1.0)"}
_BSP_START_YEAR = 2010
_FX_FIXTURE     = Path(__file__).parent.parent.parent / "data" / "fixtures" / "fx_seed.csv"

_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun",
           "jul", "aug", "sep", "oct", "nov", "dec"]

_MISSING_TOKENS = {"-", "..", "n.a.", "n/a", ""}


def _get_html(url: str) -> str:
    for attempt in range(_RETRIES):
        try:
            with httpx.Client(timeout=_TIMEOUT, headers=_HEADERS) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            if attempt == _RETRIES - 1:
                raise
            wait = _BACKOFF ** attempt
            log.warning("bsp_fetch_retry", url=url, attempt=attempt, wait=wait, error=str(exc))
            time.sleep(wait)
    raise RuntimeError("unreachable")


def _parse_rate(raw: str) -> float | None:
    cleaned = raw.strip().replace(",", "")
    if cleaned in _MISSING_TOKENS:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _load_fx_fixture() -> list[dict]:
    """Fallback: load fx_seed.csv when live BSP Table 12 returns empty."""
    if not _FX_FIXTURE.exists():
        log.warning("bsp_fx_fixture_not_found", path=str(_FX_FIXTURE))
        return []
    records = []
    with open(_FX_FIXTURE) as f:
        for row in csv.DictReader(f):
            try:
                records.append({
                    "rate_date":     row["rate_date"],
                    "currency_pair": row.get("currency_pair", "USD/PHP"),
                    "rate":          float(row["rate"]) if row["rate"] else None,
                    "source":        row.get("source", "bsp_fixture"),
                })
            except (KeyError, ValueError) as exc:
                log.warning("bsp_fx_fixture_row_error", error=str(exc))
    log.info("bsp_fx_fixture_loaded", rows=len(records))
    return records


def fetch_bsp_fx_raw() -> list[dict]:
    """Scrape BSP Table 12 — monthly USD/PHP averages.
    Falls back to fx_seed.csv fixture if live data is unavailable.
    """
    try:
        html = _get_html(_TABLE12_URL)
    except Exception as exc:
        log.error("bsp_table12_fetch_failed", error=str(exc))
        return _load_fx_fixture()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        log.warning("bsp_table12_no_table")
        return _load_fx_fixture()

    records: list[dict] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue
        try:
            year = int(cells[0])
        except (ValueError, IndexError):
            continue
        if year < _BSP_START_YEAR:
            continue

        for month_idx in range(1, 13):
            if month_idx >= len(cells):
                break
            rate = _parse_rate(cells[month_idx])
            if rate is None:
                continue
            records.append({
                "rate_date":     date(year, month_idx, 1).isoformat(),
                "currency_pair": "USD/PHP",
                "rate":          rate,
                "source":        "bsp_table12",
            })

    if not records:
        log.warning("bsp_table12_empty_using_fixture")
        return _load_fx_fixture()

    log.info("bsp_table12_fetched", records=len(records))
    return records


def fetch_bsp_policy_raw() -> list[dict]:
    """Scrape BSP monetary policy key rates page.
    Returns sorted ascending by decision_date.
    """
    html  = _get_html(_POLICY_URL)
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No table found in BSP policy rate page")

    rows_data: list[dict] = []
    prev_rate: float | None = None

    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cells) < 2:
            continue

        decision_date: date | None = None
        for fmt in ("%B %d, %Y", "%d %B %Y", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                decision_date = datetime.strptime(cells[0].strip(), fmt).date()
                break
            except ValueError:
                continue

        if decision_date is None:
            continue

        rp = _parse_rate(cells[1])
        if rp is None or not (0.5 <= rp <= 20.0):
            continue

        srp = _parse_rate(cells[2]) if len(cells) > 2 else None

        if prev_rate is None:
            direction = "hold"
        elif rp > prev_rate:
            direction = "hike"
        elif rp < prev_rate:
            direction = "cut"
        else:
            direction = "hold"

        rows_data.append({
            "decision_date": decision_date.isoformat(),
            "overnight_rp":  rp,
            "overnight_srp": srp,
            "direction":     direction,
            "source":        "bsp_monetary_policy",
        })
        prev_rate = rp

    # Enforce ascending sort and recompute directions
    rows_data.sort(key=lambda r: r["decision_date"])
    prev: float | None = None
    for row in rows_data:
        if prev is None:
            row["direction"] = "hold"
        elif row["overnight_rp"] > prev:
            row["direction"] = "hike"
        elif row["overnight_rp"] < prev:
            row["direction"] = "cut"
        else:
            row["direction"] = "hold"
        prev = row["overnight_rp"]

    log.info("bsp_policy_fetched", records=len(rows_data))
    return rows_data
