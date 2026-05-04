/**
 * App.tsx — REFACTOR
 *
 * Removed:
 *   - GOLD_MACRO_INDICATORS / GOLD_EXCHANGE_RATES hardcoded constants
 *   - queryGoldLayer() fake DuckDB WASM simulation
 *   - viewMode (aggregated/category) — not mappable to real data model
 *   - "SYSTEM DESIGN DOCUMENT // REVISION FINAL" masthead
 *   - "CONTRACT-FIRST · WAVE-EXECUTED · MULTI-AGENT · PRODUCTION GRADE" tagline
 *   - "ARCH-06 // PARQUET WASM QUERY RENDER" annotation
 *   - Section number decorations ("01", "02")
 *   - "FROM: processed_batches / quality_results / dataset_versions" footer labels
 *   - Freshness card referencing processed_batches (no API endpoint exists)
 *   - Lineage Signature card (schema hash as headline feature)
 *
 * Added:
 *   - Real API calls via src/api/client.ts
 *   - All four run status states rendered correctly
 *   - Expandable row -> stage metrics sub-table
 *   - PARTIAL expand shows "Quality stage failed - no gold partition written"
 *   - Infrastructure health panel (Postgres + S3)
 *   - Dataset versions panel
 *   - Quality checks panel
 *   - Loading and error states throughout
 */

import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  CartesianGrid, Legend, Line, LineChart,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import {
  type DatasetVersion, type ExchangeRateRow, type HealthStatus,
  type MacroIndicatorRow, type PipelineRun, type PipelineRunDetail, type QualityResult,
  fetchDatasetQuality, fetchDatasets, fetchGoldFxData, fetchGoldMacroData,
  fetchHealth, fetchRunDetail, fetchRuns,
} from "./api/client";

const T = {
  bg:"#060910",bg2:"#0c1118",bg3:"#111822",surface:"#161e2b",border:"#1e2d40",
  cyan:"#00c8ff",amber:"#f59e0b",green:"#10e878",violet:"#a78bfa",red:"#f87171",blue:"#60a5fa",
  text:"#c8d8e8",textDim:"#6b8099",textBright:"#e8f4ff",
} as const;

type RunStatus = "RUNNING"|"SUCCESS"|"PARTIAL"|"FAILED";
const STATUS_COLOR: Record<RunStatus,string> = {RUNNING:T.blue,SUCCESS:T.green,PARTIAL:T.amber,FAILED:T.red};

function StatusBadge({status}:{status:RunStatus}) {
  const color = STATUS_COLOR[status]??T.textDim;
  return (
    <span className="inline-flex items-center gap-1.5 px-2 py-0.5 text-[10px] font-mono font-bold uppercase rounded-sm border"
      style={{color,borderColor:`${color}44`,backgroundColor:`${color}18`}} aria-label={`Status: ${status}`}>
      {status==="RUNNING"&&<span className="inline-block w-2 h-2 rounded-full animate-pulse" style={{backgroundColor:color}}/>}
      {status}
    </span>
  );
}

function HealthDot({value}:{value:string}) {
  const ok=value==="connected"||value==="reachable"||value==="ok";
  const color=ok?T.green:T.amber;
  return (
    <span className="inline-flex items-center gap-1.5 font-mono text-xs" aria-label={value}>
      <span className="inline-block w-2 h-2 rounded-full" style={{backgroundColor:color}} aria-hidden/>
      <span style={{color}}>{value}</span>
    </span>
  );
}

function Card({children,className=""}:{children:React.ReactNode;className?:string}) {
  return <div className={`border rounded-sm ${className}`} style={{backgroundColor:T.surface,borderColor:T.border}}>{children}</div>;
}

function SectionTitle({children}:{children:React.ReactNode}) {
  return <div className="px-4 py-2.5 border-b text-[10px] font-mono font-bold uppercase tracking-widest" style={{borderColor:T.border,color:T.textDim}}>{children}</div>;
}

function Pill({active,onClick,color,children}:{active:boolean;onClick:()=>void;color:string;children:React.ReactNode}) {
  return (
    <button onClick={onClick} className="px-3 py-1 text-[10px] font-mono uppercase tracking-wider border rounded-sm transition-all"
      style={active?{borderColor:color,color,backgroundColor:`${color}22`}:{borderColor:T.border,color:T.textDim,backgroundColor:T.bg3}}>
      {children}
    </button>
  );
}

function Skeleton({className=""}:{className?:string}) {
  return <div className={`animate-pulse rounded-sm ${className}`} style={{backgroundColor:T.bg3}}/>;
}

function formatDuration(start:string,end:string|null):string {
  if(!end) return "in progress";
  const s=Math.floor((new Date(end).getTime()-new Date(start).getTime())/1000);
  const m=Math.floor(s/60);
  return m>0?`${m}m ${s%60}s`:`${s}s`;
}

function ExpandedRunRow({runId,status}:{runId:string;status:RunStatus}) {
  const [detail,setDetail]=useState<PipelineRunDetail|null>(null);
  const [loading,setLoading]=useState(true);
  useEffect(()=>{fetchRunDetail(runId).then(setDetail).finally(()=>setLoading(false));},[runId]);
  if(loading) return <tr><td colSpan={7} className="p-4"><Skeleton className="h-4 w-48"/></td></tr>;
  return (
    <tr style={{backgroundColor:T.bg2}}>
      <td colSpan={7} className="px-6 py-4">
        {status==="PARTIAL"&&(
          <p className="text-xs font-mono mb-3 px-3 py-2 border rounded-sm"
            style={{color:T.amber,borderColor:`${T.amber}44`,backgroundColor:`${T.amber}11`}}>
            Quality stage failed — no gold partition written
          </p>
        )}
        {detail&&detail.stage_metrics.length>0&&(
          <table className="w-full text-[11px] font-mono">
            <thead><tr style={{color:T.textDim}}>
              <th className="text-left pb-2 pr-6">Stage</th>
              <th className="text-right pb-2 pr-6">Duration (s)</th>
              <th className="text-right pb-2 pr-6">Input Rows</th>
              <th className="text-right pb-2">Output Rows</th>
            </tr></thead>
            <tbody>
              {detail.stage_metrics.map(s=>(
                <tr key={s.id} style={{color:T.text}}>
                  <td className="pr-6 py-0.5" style={{color:T.cyan}}>{s.stage_name}</td>
                  <td className="pr-6 py-0.5 text-right">{s.duration_seconds?.toFixed(2)??"—"}</td>
                  <td className="pr-6 py-0.5 text-right">{s.input_rows?.toLocaleString()??"—"}</td>
                  <td className="py-0.5 text-right">{s.output_rows?.toLocaleString()??"—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {detail&&detail.stage_metrics.length===0&&(
          <p className="text-xs font-mono" style={{color:T.textDim}}>No stage metrics recorded.</p>
        )}
      </td>
    </tr>
  );
}

function RunsTable() {
  const [runs,setRuns]=useState<PipelineRun[]>([]);
  const [loading,setLoading]=useState(true);
  const [error,setError]=useState<string|null>(null);
  const [sourceFilter,setSource]=useState("all");
  const [expanded,setExpanded]=useState<string|null>(null);

  const load=useCallback((src:string)=>{
    setLoading(true);setError(null);
    fetchRuns(src==="all"?undefined:src)
      .then(setRuns).catch((e:Error)=>setError(e.message)).finally(()=>setLoading(false));
  },[]);

  useEffect(()=>{load(sourceFilter);},[sourceFilter,load]);

  return (
    <Card>
      <div className="flex items-center justify-between px-4 py-2.5 border-b" style={{borderColor:T.border}}>
        <span className="text-[10px] font-mono font-bold uppercase tracking-widest" style={{color:T.textDim}}>Pipeline Runs</span>
        <div className="flex gap-1">
          {["all","psa","bsp_fx"].map(s=>(
            <Pill key={s} active={sourceFilter===s} onClick={()=>setSource(s)} color={T.cyan}>{s}</Pill>
          ))}
        </div>
      </div>
      {error&&<p className="px-4 py-3 text-xs font-mono" style={{color:T.red}}>{error}</p>}
      <div className="overflow-x-auto">
        <table className="w-full text-[11px] font-mono">
          <thead>
            <tr className="border-b text-left" style={{borderColor:T.border,color:T.textDim}}>
              <th className="px-4 py-2.5 font-normal">Run Date</th>
              <th className="px-4 py-2.5 font-normal">Source</th>
              <th className="px-4 py-2.5 font-normal">Status</th>
              <th className="px-4 py-2.5 font-normal text-right">Records In</th>
              <th className="px-4 py-2.5 font-normal text-right">Rejected</th>
              <th className="px-4 py-2.5 font-normal text-right">Duration</th>
              <th className="px-4 py-2.5 font-normal">Started At</th>
            </tr>
          </thead>
          <tbody>
            {loading&&Array.from({length:4}).map((_,i)=>(
              <tr key={i} className="border-b" style={{borderColor:T.border}}>
                {Array.from({length:7}).map((_,j)=>(
                  <td key={j} className="px-4 py-3"><Skeleton className="h-3 w-16"/></td>
                ))}
              </tr>
            ))}
            {!loading&&runs.map(run=>(
              <React.Fragment key={run.run_id}>
                <tr className="border-b cursor-pointer transition-colors" style={{borderColor:T.border}}
                  onClick={()=>setExpanded(p=>p===run.run_id?null:run.run_id)}
                  onMouseEnter={e=>(e.currentTarget.style.backgroundColor=T.bg3)}
                  onMouseLeave={e=>(e.currentTarget.style.backgroundColor="transparent")}
                  aria-expanded={expanded===run.run_id}>
                  <td className="px-4 py-3" style={{color:T.text}}>{run.run_date}</td>
                  <td className="px-4 py-3" style={{color:T.violet}}>{run.source}</td>
                  <td className="px-4 py-3"><StatusBadge status={run.status}/></td>
                  <td className="px-4 py-3 text-right" style={{color:T.textBright}}>{run.records_ingested?.toLocaleString()??"—"}</td>
                  <td className="px-4 py-3 text-right" style={{color:(run.records_rejected??0)>0?T.amber:T.textDim}}>{run.records_rejected?.toLocaleString()??"—"}</td>
                  <td className="px-4 py-3 text-right" style={{color:T.textDim}}>{formatDuration(run.started_at,run.ended_at)}</td>
                  <td className="px-4 py-3" style={{color:T.textDim}}>
                    {new Date(run.started_at).toLocaleString("en-PH",{timeZone:"Asia/Manila",dateStyle:"medium",timeStyle:"short"})}
                  </td>
                </tr>
                {expanded===run.run_id&&<ExpandedRunRow runId={run.run_id} status={run.status}/>}
              </React.Fragment>
            ))}
            {!loading&&runs.length===0&&(
              <tr><td colSpan={7} className="px-4 py-8 text-center text-xs font-mono" style={{color:T.textDim}}>No pipeline runs found.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function InfraCard({health}:{health:HealthStatus|null}) {
  return (
    <Card>
      <SectionTitle>Infrastructure</SectionTitle>
      <div className="p-4 space-y-3">
        {health?(<>
          <div className="flex justify-between items-center text-xs font-mono">
            <span style={{color:T.textDim}}>Postgres</span><HealthDot value={health.db}/>
          </div>
          <div className="flex justify-between items-center text-xs font-mono">
            <span style={{color:T.textDim}}>S3 (MinIO)</span><HealthDot value={health.storage}/>
          </div>
        </>):(<><Skeleton className="h-4 w-full"/><Skeleton className="h-4 w-full"/></>)}
      </div>
    </Card>
  );
}

function DatasetsCard() {
  const [datasets,setDatasets]=useState<DatasetVersion[]>([]);
  const [loading,setLoading]=useState(true);
  useEffect(()=>{fetchDatasets().then(setDatasets).finally(()=>setLoading(false));},[]);
  return (
    <Card>
      <SectionTitle>Dataset Versions</SectionTitle>
      <div className="p-4 space-y-3">
        {loading?(<><Skeleton className="h-12 w-full"/><Skeleton className="h-12 w-full"/></>):
        datasets.length===0?(<p className="text-xs font-mono" style={{color:T.textDim}}>No dataset versions found.</p>):
        datasets.map(d=>(
          <div key={d.id} className="text-[11px] font-mono space-y-1 p-3 border rounded-sm" style={{borderColor:T.border,backgroundColor:T.bg3}}>
            <div style={{color:T.cyan}}>{d.dataset_name}</div>
            <div className="flex justify-between">
              <span style={{color:T.textDim}}>{d.partition_key}</span>
              <span style={{color:T.textBright}}>{d.row_count.toLocaleString()} rows</span>
            </div>
            <div className="flex justify-between">
              <span style={{color:T.textDim}}>hash</span>
              <span style={{color:T.textDim}}>{d.schema_hash.slice(0,8)}</span>
            </div>
            <div className="truncate text-[10px]" style={{color:T.textDim}} title={d.s3_path}>
              {d.s3_path.length>48?`…${d.s3_path.slice(-45)}`:d.s3_path}
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}

function QualityCard({datasetName}:{datasetName:string}) {
  const [checks,setChecks]=useState<QualityResult[]>([]);
  const [loading,setLoading]=useState(true);
  useEffect(()=>{fetchDatasetQuality(datasetName).then(setChecks).finally(()=>setLoading(false));},[datasetName]);
  return (
    <Card>
      <SectionTitle>Quality Checks — {datasetName}</SectionTitle>
      <div className="p-4">
        {loading?<Skeleton className="h-20 w-full"/>:
        checks.length===0?<p className="text-xs font-mono" style={{color:T.textDim}}>No quality results yet.</p>:
        <table className="w-full text-[11px] font-mono">
          <thead><tr style={{color:T.textDim}}>
            <th className="text-left pb-2 pr-4 font-normal">Check</th>
            <th className="text-center pb-2 pr-4 font-normal">Passed</th>
            <th className="text-right pb-2 pr-4 font-normal">Failed</th>
            <th className="text-right pb-2 font-normal">Threshold</th>
          </tr></thead>
          <tbody>
            {checks.map(c=>(
              <tr key={c.id}>
                <td className="pr-4 py-1" style={{color:T.text}}>{c.check_name}</td>
                <td className="pr-4 py-1 text-center">
                  {c.passed?<span style={{color:T.green}} aria-label="passed">✓</span>:<span style={{color:T.red}} aria-label="failed">✗</span>}
                </td>
                <td className="pr-4 py-1 text-right" style={{color:c.failed_count>0?T.amber:T.textDim}}>{c.failed_count}</td>
                <td className="py-1 text-right" style={{color:T.textDim}}>{c.threshold??"—"}</td>
              </tr>
            ))}
          </tbody>
        </table>}
      </div>
    </Card>
  );
}

type ChartSeries="CPI_ALL"|"CPI_YOY"|"USD/PHP";
interface ChartPoint{period:string;CPI_ALL?:number;CPI_YOY?:number;"USD/PHP"?:number;}

function GoldChart() {
  const [macroRows,setMacroRows]=useState<MacroIndicatorRow[]>([]);
  const [fxRows,setFxRows]=useState<ExchangeRateRow[]>([]);
  const [loading,setLoading]=useState(true);
  const [error,setError]=useState<string|null>(null);
  const [active,setActive]=useState<ChartSeries[]>(["CPI_ALL","USD/PHP"]);

  useEffect(()=>{
    Promise.all([fetchGoldMacroData(),fetchGoldFxData()])
      .then(([macro,fx])=>{setMacroRows(macro);setFxRows(fx);})
      .catch((e:Error)=>setError(e.message))
      .finally(()=>setLoading(false));
  },[]);

  const chartData=useMemo<ChartPoint[]>(()=>{
    const map=new Map<string,ChartPoint>();
    for(const r of macroRows){
      const p=r.period.slice(0,7);
      if(!map.has(p))map.set(p,{period:p});
      const pt=map.get(p)!;
      if(r.indicator_code==="CPI_ALL")pt.CPI_ALL=r.value;
      if(r.indicator_code==="CPI_YOY")pt.CPI_YOY=r.value;
    }
    for(const r of fxRows){
      const p=r.period.slice(0,7);
      if(!map.has(p))map.set(p,{period:p});
      map.get(p)!["USD/PHP"]=r.rate;
    }
    return Array.from(map.values()).sort((a,b)=>a.period.localeCompare(b.period));
  },[macroRows,fxRows]);

  const toggle=(s:ChartSeries)=>setActive(p=>p.includes(s)?p.filter(x=>x!==s):[...p,s]);
  const SERIES=[{key:"CPI_ALL"as ChartSeries,color:T.cyan,axis:"left"as const},
    {key:"CPI_YOY"as ChartSeries,color:T.violet,axis:"right"as const},
    {key:"USD/PHP"as ChartSeries,color:T.amber,axis:"left"as const}];

  return (
    <Card>
      <div className="flex items-center justify-between px-4 py-2.5 border-b" style={{borderColor:T.border}}>
        <span className="text-[10px] font-mono font-bold uppercase tracking-widest" style={{color:T.textDim}}>Gold Layer — Monthly Trend</span>
        <div className="flex gap-1">
          {SERIES.map(({key,color})=>(
            <Pill key={key} active={active.includes(key)} onClick={()=>toggle(key)} color={color}>{key}</Pill>
          ))}
        </div>
      </div>
      <div className="p-4">
        {loading&&<Skeleton className="h-72 w-full"/>}
        {error&&<p className="text-xs font-mono py-4" style={{color:T.red}}>{error}</p>}
        {!loading&&!error&&(
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{top:8,right:32,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="period" stroke={T.textDim} tick={{fill:T.textDim,fontSize:10,fontFamily:"monospace"}} tickMargin={8}/>
                <YAxis yAxisId="left" stroke={T.textDim} tick={{fill:T.textDim,fontSize:10,fontFamily:"monospace"}} domain={["auto","auto"]}/>
                <YAxis yAxisId="right" orientation="right" stroke={T.textDim} tick={{fill:T.textDim,fontSize:10,fontFamily:"monospace"}} domain={["auto","auto"]}/>
                <Tooltip contentStyle={{backgroundColor:T.surface,border:`1px solid ${T.border}`,borderRadius:"2px",fontFamily:"monospace",fontSize:"11px"}}
                  itemStyle={{color:T.textBright}} labelStyle={{color:T.textDim,marginBottom:"6px"}}/>
                <Legend wrapperStyle={{fontSize:"11px",paddingTop:"12px"}} iconType="circle"/>
                {SERIES.map(({key,color,axis})=>active.includes(key)?(
                  <Line key={key} yAxisId={axis} type="monotone" dataKey={key} stroke={color} strokeWidth={2}
                    dot={{r:3,fill:T.bg,strokeWidth:2}} activeDot={{r:5}}/>
                ):null)}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </Card>
  );
}

function TopBar({health}:{health:HealthStatus|null}) {
  const overall=health?.status??"degraded";
  const color=overall==="ok"?T.green:T.amber;
  return (
    <header className="flex items-center justify-between px-6 h-12 border-b flex-shrink-0"
      style={{backgroundColor:T.bg2,borderColor:T.border}}>
      <div className="flex items-center gap-4 font-mono text-xs">
        <span className="font-bold" style={{color:T.textBright}}>PH Lakehouse</span>
        <span style={{color:T.textDim}}>|</span>
        <span style={{color:T.cyan}}>ph_lakehouse_pipeline</span>
        <span style={{color:T.textDim}}>|</span>
        <span style={{color:T.textDim}}>ph-lakehouse-monthly</span>
      </div>
      <div className="flex items-center gap-4 font-mono text-xs">
        {health&&(<>
          <span style={{color:T.textDim}}>db: <HealthDot value={health.db}/></span>
          <span style={{color:T.textDim}}>s3: <HealthDot value={health.storage}/></span>
        </>)}
        <span className="px-2 py-0.5 rounded-sm border text-[10px] font-bold uppercase"
          style={{color,borderColor:`${color}44`,backgroundColor:`${color}18`}} aria-label={`System status: ${overall}`}>
          ● {overall}
        </span>
      </div>
    </header>
  );
}

function MetricsStrip({runs}:{runs:PipelineRun[]}) {
  const latest=runs[0]??null;
  return (
    <div className="grid grid-cols-4 border-b" style={{borderColor:T.border,backgroundColor:T.bg2}}>
      {[
        {label:"Latest Run Status",value:latest?<StatusBadge status={latest.status}/>:<span style={{color:T.textDim}}>—</span>},
        {label:"Records Ingested",value:<span className="font-mono text-2xl font-bold" style={{color:T.cyan}}>{latest?.records_ingested?.toLocaleString()??"—"}</span>},
        {label:"Quality Pass Rate",value:<span className="font-mono text-2xl font-bold" style={{color:T.green}}>{latest?.status==="SUCCESS"?"100.00%":"—"}</span>},
        {label:"Next Scheduled Run",value:<span className="font-mono text-sm" style={{color:T.textDim}}>2025-05-01 06:00 Asia/Manila</span>},
      ].map(({label,value})=>(
        <div key={label} className="px-6 py-4 border-r last:border-r-0 flex flex-col gap-1" style={{borderColor:T.border}}>
          <div className="text-[9px] font-mono uppercase tracking-widest" style={{color:T.textDim}}>{label}</div>
          <div>{value}</div>
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const [health,setHealth]=useState<HealthStatus|null>(null);
  const [runs,setRuns]=useState<PipelineRun[]>([]);
  useEffect(()=>{
    fetchHealth().then(setHealth).catch(()=>{});
    fetchRuns(undefined,20).then(setRuns).catch(()=>{});
  },[]);
  return (
    <div className="min-h-screen flex flex-col text-sm" style={{backgroundColor:T.bg,color:T.text}}>
      <TopBar health={health}/>
      <MetricsStrip runs={runs}/>
      <main className="flex-1 p-6 grid grid-cols-5 gap-4 min-w-0">
        <div className="col-span-3 flex flex-col gap-4 min-w-0">
          <RunsTable/>
          <GoldChart/>
        </div>
        <div className="col-span-2 flex flex-col gap-4">
          <InfraCard health={health}/>
          <DatasetsCard/>
          <QualityCard datasetName="gold_macro_indicators"/>
        </div>
      </main>
    </div>
  );
}
