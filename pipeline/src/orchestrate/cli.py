"""
Backfill CLI — monthly granularity.

Usage:
    make backfill START=2023-01 END=2023-03
    python -m src.orchestrate.cli backfill --start 2023-01 --end 2023-03

3-month backfill = 3 Spark executions against 3 partitions, not 90 daily runs.
"""
from __future__ import annotations

from datetime import date

import typer
from dateutil.relativedelta import relativedelta

from src.orchestrate.flows import lakehouse_pipeline

app = typer.Typer()


@app.command()
def backfill(
    start: str = typer.Option(..., help="Start month inclusive: YYYY-MM"),
    end:   str = typer.Option(..., help="End month inclusive: YYYY-MM"),
) -> None:
    """
    Reprocess all months in [start, end] inclusive.

    Bypasses the processed_batches idempotency guard so existing partitions
    are overwritten. This is the correct behavior for backfills — partition
    overwrite is safe because partitionOverwriteMode=dynamic is active.
    """
    start_date = date.fromisoformat(f"{start}-01")
    end_date   = date.fromisoformat(f"{end}-01")
    current    = start_date

    typer.echo(f"Backfill from {start} to {end}")

    while current <= end_date:
        typer.echo(f"  processing {current.strftime('%Y-%m')}")
        lakehouse_pipeline(run_date=current, backfill=True)
        current += relativedelta(months=1)

    typer.echo("Backfill complete.")


if __name__ == "__main__":
    app()
