from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
from datetime import datetime
import csv
import io
import zipfile
from google.cloud import bigquery
import json

from .bq import BigQueryService
from .embeddings import EmbeddingMode, EmbeddingService
from .kpi import KPIService
from .models import (
	DatasetResponse,
	TableRef,
	TableInfoResponse,
	PrepareRequest,
	PrepareResponse,
	GenerateKpisRequest,
	GenerateKpisResponse,
	RunKpiRequest,
	RunKpiResponse,
	DashboardSaveRequest,
	DashboardSaveResponse,
	DashboardListResponse,
	DashboardGetResponse,
	KPICatalogAddRequest,
	KPICatalogListResponse,
)
from .diagnostics import run_self_test
from .llm import LLMClient

llm_client = LLMClient()

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET_EMBED = os.getenv("BQ_EMBEDDINGS_DATASET", "analytics_poc")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
EMBEDDING_MODE = os.getenv("EMBEDDING_MODE", "bigquery")
CREATE_INDEX_THRESHOLD = int(os.getenv("CREATE_INDEX_THRESHOLD", "5000"))
DASH_DATASET = os.getenv("DASHBOARDS_DATASET", "analytics_dash")

app = FastAPI(title="Analytics KPI POC")

# CORS for local dev
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)

bq_service = BigQueryService(project_id=PROJECT_ID, location=BQ_LOCATION)
embedding_service = EmbeddingService(
	mode=EmbeddingMode(EMBEDDING_MODE),
	project_id=PROJECT_ID,
	location=BQ_LOCATION,
	bq_dataset=BQ_DATASET_EMBED,
)
kpi_service = KPIService(
	bq=bq_service,
	embeddings=embedding_service,
	project_id=PROJECT_ID,
	embedding_dataset=BQ_DATASET_EMBED,
	create_index_threshold=CREATE_INDEX_THRESHOLD,
)


@app.get("/api/health")
def health() -> Dict[str, str]:
	return {"status": "ok"}


@app.get("/api/datasets", response_model=DatasetResponse)
def list_datasets():
	try:
		datasets = bq_service.list_datasets()
		return {"datasets": datasets}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/datasets/{dataset_id}/tables", response_model=TableInfoResponse)
def list_tables(dataset_id: str):
	try:
		tables = bq_service.list_tables(dataset_id)
		return {"dataset_id": dataset_id, "tables": tables}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/prepare", response_model=PrepareResponse)
def prepare(req: PrepareRequest):
	try:
		result = kpi_service.prepare_tables(req.tables, sample_rows=req.sampleRows or 5)
		return {"status": "ok", "prepared": result}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/generate_kpis", response_model=GenerateKpisResponse)
def generate_kpis(req: GenerateKpisRequest):
	try:
		k = req.k or 5
		kpis = kpi_service.generate_kpis(req.tables, k=k)
		return {"kpis": kpis}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/run_kpi", response_model=RunKpiResponse)
def run_kpi(req: RunKpiRequest):
	try:
		sql = req.sql
		params: List[bigquery.ScalarQueryParameter] = []
		where_clauses: List[str] = []
		# apply date filter if provided
		if req.date_column and req.filters and isinstance(req.filters.get('date'), dict):
			date_filter = req.filters['date']
			if date_filter.get('from'):
				where_clauses.append(f"{req.date_column} >= @fromDate")
				params.append(bigquery.ScalarQueryParameter("fromDate", "DATE", date_filter['from']))
			if date_filter.get('to'):
				where_clauses.append(f"{req.date_column} <= @toDate")
				params.append(bigquery.ScalarQueryParameter("toDate", "DATE", date_filter['to']))
		# apply categorical cross-filter if provided
		if req.filters and isinstance(req.filters.get('category'), dict):
			cat = req.filters['category']
			col = cat.get('column')
			val = cat.get('value')
			if col and val is not None:
				where_clauses.append(f"{col} = @catValue")
				params.append(bigquery.ScalarQueryParameter("catValue", "STRING", str(val)))
		if where_clauses:
			wrapped = f"SELECT * FROM ( {sql} ) WHERE " + " AND ".join(where_clauses)
			job_config = bigquery.QueryJobConfig(query_parameters=params)
			rows_iter = bq_service.client.query(wrapped, job_config=job_config, location=bq_service.location)
			result_rows = [dict(r) for r in rows_iter]
			# normalize
			norm = [ { k: bq_service._normalize_value(v) for k, v in r.items() } for r in result_rows ]
			return {"rows": norm}
		rows = bq_service.query_rows(sql)
		return {"rows": rows}
	except Exception as exc:
		raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/sql/edit")
def edit_sql(payload: Dict[str, str]):
	try:
		original_sql = payload.get('sql', '')
		instruction = payload.get('instruction', '')
		new_sql = kpi_service.llm.edit_sql(original_sql, instruction)
		return {"sql": new_sql}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.post("/api/kpi/edit")
def edit_kpi(payload: Dict[str, str]):
	try:
		original_kpi = payload.get('kpi') or {}
		original_sql = original_kpi.get('sql') or payload.get('sql', '')
		instruction = payload.get('instruction', '')
		# Ask for updated kpi + markdown explanation
		system = (
			"You are a KPI editing assistant. Return JSON with two keys: 'kpi' and 'markdown'. "
			"'kpi' should include updated fields (name, short_description, chart_type, expected_schema, engine, vega_lite_spec, sql, filter_date_column). "
			"'markdown' is a readable explanation of the change (no JSON)."
		)
		user = json.dumps({"kpi": original_kpi, "sql": original_sql, "instruction": instruction})
		resp = llm_client.generate_json(system, user)
		updated_kpi = original_kpi.copy()
		if isinstance(resp, dict):
			maybe_kpi = resp.get('kpi') or {}
			if isinstance(maybe_kpi, dict):
				for key in ["name","short_description","chart_type","expected_schema","engine","vega_lite_spec","sql","filter_date_column"]:
					if key in maybe_kpi:
						updated_kpi[key] = maybe_kpi[key]
			markdown = resp.get('markdown') or ''
		else:
			markdown = ""
		# If user intends a 'card' chart, ensure vega spec is removed
		if (updated_kpi.get('chart_type') or '').lower() == 'card':
			updated_kpi['engine'] = 'none'
			updated_kpi['vega_lite_spec'] = None
		# Fallback: if no change produced, try SQL-only edit
		if updated_kpi.get('sql') == original_sql or not updated_kpi.get('sql'):
			try:
				new_sql = kpi_service.llm.edit_sql(original_sql, instruction)
				if new_sql:
					updated_kpi['sql'] = new_sql
			except Exception:
				pass
		return {"kpi": updated_kpi, "markdown": markdown or ""}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.post("/api/kpi/edit_chat")
def edit_kpi_chat(payload: Dict[str, Any]):
	"""
	Interactive KPI refinement. Body: { kpi, message, history?: [{role, content}], context?: { rows?: any[] } }
	Returns: { reply: markdown, kpi?: updated }
	"""
	try:
		kpi = payload.get('kpi') or {}
		message = payload.get('message') or ''
		history = payload.get('history') or []
		ctx = payload.get('context') or {}
		sys = (
			"You are a KPI editing assistant working with a user to refine one KPI. "
			"Use the conversation history and the current KPI to suggest improvements. "
			"When appropriate, propose changes and return JSON with keys 'markdown' (the readable response) and optional 'kpi' (the updated KPI)."
		)
		user = json.dumps({"kpi": kpi, "message": message, "history": history[-10:], "context": ctx})
		resp = llm_client.generate_json(sys, user)
		markdown = ""
		updated = None
		if isinstance(resp, dict):
			markdown = resp.get('markdown') or resp.get('text') or ""
			maybe_k = resp.get('kpi')
			if isinstance(maybe_k, dict):
				updated = maybe_k
		return {"reply": markdown or "", "kpi": updated}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.post("/api/dashboards/default")
def set_default_dashboard(payload: Dict[str, str]):
	try:
		did = payload.get('id')
		if not did:
			raise HTTPException(status_code=400, detail="id required")
		bq_service.set_default_dashboard(did, dataset_id=DASH_DATASET)
		return {"status": "ok"}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.get("/api/dashboards/default")
def get_default_dashboard():
	try:
		did = bq_service.get_default_dashboard(dataset_id=DASH_DATASET)
		if not did:
			return {"id": None}
		row = bq_service.get_dashboard(dashboard_id=did, dataset_id=DASH_DATASET)
		return {"id": did if row else None}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

# Dashboard APIs
@app.post("/api/dashboards", response_model=DashboardSaveResponse)
def save_dashboard(req: DashboardSaveRequest):
	try:
		# serialize KPI Pydantic models to dicts
		kpis = [k.model_dump() if hasattr(k, 'model_dump') else dict(k) for k in req.kpis]
		layout = req.layout
		layouts = req.layouts
		selected = [s.model_dump() if hasattr(s, 'model_dump') else dict(s) for s in req.selected_tables]
		did, ver = bq_service.save_dashboard(
			name=req.name,
			kpis=kpis,
			layout=layout,
			layouts=layouts,
			selected_tables=selected,
			global_filters=req.global_filters,
			theme=req.theme,
			version=req.version,
			dashboard_id=req.id,
			dataset_id=DASH_DATASET,
			tabs=[t.model_dump() if hasattr(t, 'model_dump') else dict(t) for t in (req.tabs or [])],
			tab_layouts=req.tab_layouts,
			last_active_tab=req.last_active_tab,
			hidden=req.hidden,
		)
		return {"id": did, "name": req.name, "version": ver}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.get("/api/dashboards", response_model=DashboardListResponse)
def list_dashboards():
	try:
		rows = bq_service.list_dashboards(dataset_id=DASH_DATASET)
		return {"dashboards": rows}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.get("/api/dashboards/{dashboard_id}", response_model=DashboardGetResponse)
def get_dashboard(dashboard_id: str):
	try:
		row = bq_service.get_dashboard(dashboard_id=dashboard_id, dataset_id=DASH_DATASET)
		if not row:
			raise HTTPException(status_code=404, detail="Dashboard not found")
		return row
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.delete("/api/dashboards/{dashboard_id}")
def delete_dashboard(dashboard_id: str, all_versions: bool = False):
	try:
		name = None
		if all_versions:
			row = bq_service.get_dashboard(dashboard_id=dashboard_id, dataset_id=DASH_DATASET)
			if row:
				name = row.get("name")
			else:
				raise HTTPException(status_code=404, detail="Dashboard not found")
		count = bq_service.delete_dashboard(dataset_id=DASH_DATASET, dashboard_id=None if all_versions else dashboard_id, name=name, all_versions=all_versions)
		return {"deleted": count}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

# Serve built SPA (Dockerfile copies frontend/dist to /app/static)
static_dir = os.path.abspath(os.getenv("STATIC_DIR", "/app/static"))
assets_dir = os.path.join(static_dir, "assets")
if os.path.isdir(assets_dir):
	app.mount("/assets", StaticFiles(directory=assets_dir, html=False), name="assets")

# Serve index.html at root and for deep links (non-API)
@app.get("/")
def index():
	index_path = os.path.join(static_dir, "index.html")
	if os.path.isfile(index_path):
		return FileResponse(index_path)
	raise HTTPException(status_code=404, detail="Not Found")

@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
	# Let API routes 404 as-is
	if full_path.startswith("api/") or full_path == "api":
		raise HTTPException(status_code=404, detail="Not Found")
	index_path = os.path.join(static_dir, "index.html")
	if os.path.isfile(index_path):
		return FileResponse(index_path)
	raise HTTPException(status_code=404, detail="Not Found")