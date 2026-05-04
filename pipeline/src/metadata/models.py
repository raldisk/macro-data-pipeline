"""
Pydantic models for Postgres metadata rows.
Used by repository.py and API response models.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class PipelineRunRow(BaseModel):
    run_id:           UUID
    pipeline_name:    str
    source:           str
    run_date:         date
    started_at:       datetime
    ended_at:         Optional[datetime]
    status:           str
    records_ingested: Optional[int]
    records_rejected: Optional[int]
    error_message:    Optional[str]


class StageMetricRow(BaseModel):
    id:               int
    run_id:           UUID
    stage_name:       str
    started_at:       datetime
    duration_seconds: Optional[float]
    input_rows:       Optional[int]
    output_rows:      Optional[int]


class QualityResultRow(BaseModel):
    id:           int
    run_id:       UUID
    check_name:   str
    passed:       bool
    failed_count: int
    threshold:    Optional[float]


class DatasetVersionRow(BaseModel):
    id:            int
    run_id:        UUID
    dataset_name:  str
    partition_key: str
    row_count:     int
    schema_hash:   str
    s3_path:       str
    created_at:    datetime


class ProcessedBatchRow(BaseModel):
    source:       str
    batch_date:   date
    run_id:       Optional[UUID]
    processed_at: datetime


class PipelineRunDetail(PipelineRunRow):
    stage_metrics:   list[StageMetricRow] = []
    quality_results: list[QualityResultRow] = []


class LineageRow(BaseModel):
    run_id:       UUID
    source:       str
    run_date:     date
    dataset_name: str
    row_count:    int
    s3_path:      str
