/**
 * api/client.ts — ADAPTER_REQUIRED
 *
 * Typed client for the PH Lakehouse Metadata API (FastAPI, port 8000).
 * All requests route through Vite's dev proxy (/api → localhost:8000)
 * so the dashboard never hard-codes a hostname.
 *
 * Responsibilities:
 *   - Owns all fetch() calls — App.tsx has no raw fetch()
 *   - Maps FastAPI response shapes to typed dashboard models
 *   - Centralises error handling and serialisation quirks (UUID strings, ISO dates)
 */

const BASE = import.meta.env.VITE_API_URL ?? "/api";

// ─── Domain types (mirrors models.py) ────────────────────────────────────────

export interface HealthStatus {
  status: "ok" | "degraded";
  db: string;
  storage: string;
}

export interface PipelineRun {
  run_id: string;
  pipeline_name: string;
  source: string;
  run_date: string;       // ISO date string
  started_at: string;     // ISO datetime string
  ended_at: string | null;
  status: "RUNNING" | "SUCCESS" | "PARTIAL" | "FAILED";
  records_ingested: number | null;
  records_rejected: number | null;
  // error_message intentionally omitted — always null in this pipeline
}

export interface StageMetric {
  id: number;
  run_id: string;
  stage_name: string;
  started_at: string;
  duration_seconds: number | null;
  input_rows: number | null;
  output_rows: number | null;
}

export interface QualityResult {
  id: number;
  run_id: string;
  check_name: string;
  passed: boolean;
  failed_count: number;
  threshold: number | null;
}

export interface PipelineRunDetail extends PipelineRun {
  stage_metrics: StageMetric[];
  quality_results: QualityResult[];
}

export interface DatasetVersion {
  id: number;
  run_id: string;
  dataset_name: string;
  partition_key: string;
  row_count: number;
  schema_hash: string;
  s3_path: string;
  created_at: string;
}

// Gold layer row shapes — driven by contracts/gold_*.yaml
export interface MacroIndicatorRow {
  period: string;           // ISO date
  indicator_code: string;   // CPI_ALL | CPI_YOY
  value: number;
  source: string;
}

export interface ExchangeRateRow {
  period: string;
  currency_pair: string;    // USD/PHP
  rate: number;
  source: string;
}

export interface GoldDataResponse<T> {
  dataset: string;
  rows: T[];
  count: number;
}

// ─── Internal fetch helper ────────────────────────────────────────────────────

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API ${path} → ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

// ─── Public API ───────────────────────────────────────────────────────────────

export async function fetchHealth(): Promise<HealthStatus> {
  return apiFetch<HealthStatus>("/health");
}

export async function fetchRuns(source?: string, limit = 20): Promise<PipelineRun[]> {
  const qs = new URLSearchParams();
  if (source) qs.set("source", source);
  qs.set("limit", String(limit));
  return apiFetch<PipelineRun[]>(`/runs?${qs}`);
}

export async function fetchRunDetail(runId: string): Promise<PipelineRunDetail> {
  return apiFetch<PipelineRunDetail>(`/runs/${runId}`);
}

export async function fetchDatasets(): Promise<DatasetVersion[]> {
  return apiFetch<DatasetVersion[]>("/datasets");
}

export async function fetchDatasetQuality(name: string): Promise<QualityResult[]> {
  return apiFetch<QualityResult[]>(`/datasets/${name}/quality`);
}

export async function fetchGoldMacroData(): Promise<MacroIndicatorRow[]> {
  const res = await apiFetch<GoldDataResponse<MacroIndicatorRow>>(
    "/gold/gold_macro_indicators/data"
  );
  return res.rows;
}

export async function fetchGoldFxData(): Promise<ExchangeRateRow[]> {
  const res = await apiFetch<GoldDataResponse<ExchangeRateRow>>(
    "/gold/gold_exchange_rates/data"
  );
  return res.rows;
}
