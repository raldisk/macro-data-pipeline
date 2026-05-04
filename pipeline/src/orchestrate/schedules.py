"""
Prefect deployment and schedule configuration — Prefect 3 API.

Monthly schedule: 1st of each month at 06:00 Asia/Manila (UTC+8).
Deploy with: python src/orchestrate/schedules.py
"""
from __future__ import annotations

from src.orchestrate.flows import lakehouse_pipeline


if __name__ == "__main__":
    # Prefect 3: flow.serve() manages the schedule and keeps the process alive.
    # For worker-based deployment instead, use `prefect deploy` via the CLI
    # with a prefect.yaml manifest.
    lakehouse_pipeline.serve(
        name            = "ph-lakehouse-monthly",
        cron            = "0 6 1 * *",
        timezone        = "Asia/Manila",
        parameters      = {"backfill": False},
        tags            = ["ph-lakehouse", "production"],
        pause_on_shutdown = False,
    )
