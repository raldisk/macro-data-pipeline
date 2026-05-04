"""
Contract YAML loader — Wave 1 artifact.
Parses contracts/*.yaml into typed Contract dataclasses.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any

import yaml

from src.transform.quality import CheckConfig

_CONTRACTS_DIR = pathlib.Path(__file__).parent.parent.parent / "contracts"


@dataclass
class Contract:
    dataset: str
    version: int
    partition_key: list[str]
    schema: dict[str, str]
    quality_checks: list[CheckConfig]
    quarantine_on_failure: bool
    hard_failure_threshold: float


def load_contract(name: str) -> Contract:
    """
    Load a gold contract YAML by dataset name.

    Args:
        name: e.g. "gold_macro_indicators" or "gold_exchange_rates"

    Returns:
        Contract dataclass with typed quality_checks
    """
    path = _CONTRACTS_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Contract not found: {path}")

    with open(path) as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    checks = [
        CheckConfig(
            check   = c["check"],
            columns = c.get("columns"),
            column  = c.get("column"),
            value   = c.get("value"),
            min     = c.get("min"),
            max     = c.get("max"),
        )
        for c in raw.get("quality_checks", [])
    ]

    return Contract(
        dataset                 = raw["dataset"],
        version                 = raw["version"],
        partition_key           = raw["partition_key"],
        schema                  = raw["schema"],
        quality_checks          = checks,
        quarantine_on_failure   = raw.get("quarantine_on_failure", True),
        hard_failure_threshold  = raw.get("hard_failure_threshold", 0.05),
    )
