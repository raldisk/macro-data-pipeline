"""
FastAPI metadata API — read-only.

Reads from Postgres only.
No S3 reads. No analytics queries. No data serving.
Does NOT duplicate DuckDB/FastAPI Repo A endpoints.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import datasets, gold, health, runs

app = FastAPI(
    title       = "PH Lakehouse Metadata API",
    description = "Pipeline run metadata, lineage, and quality results. Read-only.",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET"],
    allow_headers  = ["*"],
)

app.include_router(health.router,   tags=["health"])
app.include_router(runs.router,     prefix="/runs",     tags=["runs"])
app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
app.include_router(gold.router,     prefix="/gold",     tags=["gold"])
