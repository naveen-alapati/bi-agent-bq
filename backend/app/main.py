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
		kpis = kpi_service.generate_kpis(req.tables, k=k, prefer_cross=bool(getattr(req, 'prefer_cross', False)))
		return {"kpis": kpis}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/generate_custom_kpi")
def generate_custom_kpi(payload: Dict[str, Any]):
	"""
	Generate a custom KPI based on user description and selected tables.
	Body: { tables: List[TableRef], description: str, clarifying_questions?: List[str], answers?: List[str] }
	Returns: { kpi: KPIItem, sql: str, chart_type: str, vega_lite_spec: dict }
	"""
	try:
		tables = payload.get('tables', [])
		description = payload.get('description', '')
		clarifying_questions = payload.get('clarifying_questions', [])
		answers = payload.get('answers', [])
		
		if not tables or not description:
			raise HTTPException(status_code=400, detail="Tables and description are required")
		
		# If we have clarifying questions but no answers, return the questions
		if clarifying_questions and not answers:
			return {"clarifying_questions": clarifying_questions}
		
		# Generate the custom KPI
		kpi_result = kpi_service.generate_custom_kpi(tables, description, answers)
		
		return {
			"kpi": kpi_result,
			"sql": kpi_result.sql,
			"chart_type": kpi_result.chart_type,
			"vega_lite_spec": kpi_result.vega_lite_spec
		}
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
		# Helper to run query (with optional WHERE wrapper)
		def _run_query(q: str):
			if where_clauses:
				wrapped = f"SELECT * FROM ( {q} ) WHERE " + " AND ".join(where_clauses)
				job_config = bigquery.QueryJobConfig(query_parameters=params)
				rows_iter = bq_service.client.query(wrapped, job_config=job_config, location=bq_service.location)
				result_rows = [dict(r) for r in rows_iter]
				return [ { k: bq_service._normalize_value(v) for k, v in r.items() } for r in result_rows ]
			rows = bq_service.query_rows(q)
			return rows
		# First attempt
		try:
			rows = _run_query(sql)
			return {"rows": rows}
		except Exception as inner_exc:
			msg = str(inner_exc).lower()
			# BigQuery division-by-zero errors may mention divide by zero / invalid / safe divide
			if "divide by zero" in msg or "division by zero" in msg:
				try:
					# Ask LLM to rewrite with SAFE_DIVIDE while preserving schema/aliases
					fixed_sql = kpi_service.llm.edit_sql(sql, "Rewrite to use SAFE_DIVIDE for all divisions; preserve output columns and aliases.")
					rows = _run_query(fixed_sql)
					return {"rows": rows}
				except Exception:
					# Fall through to raise original error if retry fails
					pass
			# If not a divide-by-zero or retry failed, rethrow the original
			raise inner_exc
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
					if key in maybe_kpi and maybe_kpi[key] is not None:
						updated_kpi[key] = maybe_kpi[key]
			markdown = resp.get('markdown') or ''
		else:
			markdown = ""
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



@app.delete("/api/dashboards/{dashboard_id}")
def delete_dashboard(dashboard_id: str):
	try:
		bq_service.delete_dashboard(dashboard_id, dataset_id=DASH_DATASET)
		return {"status": "ok"}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.get("/api/dashboards/most-recent")
def get_most_recent_dashboard():
	try:
		did = bq_service.get_most_recent_dashboard(dataset_id=DASH_DATASET)
		return {"id": did}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/export/card")
def export_card(payload: Dict[str, Any]):
	try:
		sql = payload.get('sql', '')
		rows = bq_service.query_rows(sql)
		# CSV export
		output = io.StringIO()
		writer = None
		for r in rows:
			if writer is None:
				writer = csv.DictWriter(output, fieldnames=list(r.keys()))
				writer.writeheader()
			writer.writerow(r)
		output.seek(0)
		return StreamingResponse(iter([output.getvalue()]), media_type='text/csv', headers={'Content-Disposition': 'attachment; filename="card.csv"'})
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/export/dashboard")
def export_dashboard(payload: Dict[str, Any]):
	try:
		kpis = payload.get('kpis', [])
		archive = io.BytesIO()
		with zipfile.ZipFile(archive, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
			for i, k in enumerate(kpis):
				sql = k.get('sql', '')
				rows = bq_service.query_rows(sql)
				csv_buf = io.StringIO()
				writer = None
				for r in rows:
					if writer is None:
						writer = csv.DictWriter(csv_buf, fieldnames=list(r.keys()))
						writer.writeheader()
					writer.writerow(r)
				csv_content = csv_buf.getvalue()
				zf.writestr(f"card_{i+1}.csv", csv_content)
		archive.seek(0)
		return StreamingResponse(archive, media_type='application/zip', headers={'Content-Disposition': 'attachment; filename="dashboard.zip"'})
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/selftest")
def api_selftest(dataset: Optional[str] = None, limit_tables: int = 3, sample_rows: int = 3, k: int = 3, run_kpis_limit: int = 2, force_llm: bool = True):
	try:
		report = run_self_test(bq_service, kpi_service, dataset=dataset, limit_tables=limit_tables, sample_rows=sample_rows, kpis_k=k, run_kpis_limit=run_kpis_limit, force_llm=force_llm)
		return report
	except Exception as exc:
		return {"error": str(exc)}


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


@app.post("/api/kpi_catalog", response_model=Dict[str, Any])
def kpi_catalog_add(req: KPICatalogAddRequest):
	try:
		items = []
		for k in req.kpis:
			item = k.model_dump() if hasattr(k, 'model_dump') else dict(k)
			item['dataset_id'] = req.datasetId
			item['table_id'] = req.tableId
			item['tags'] = {"datasetId": req.datasetId, "tableId": req.tableId}
			items.append(item)
		count = bq_service.add_to_kpi_catalog(items, dataset_id=DASH_DATASET)
		return {"inserted": count}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/kpi_catalog", response_model=KPICatalogListResponse)
def kpi_catalog_list(datasetId: Optional[str] = None, tableId: Optional[str] = None):
	try:
		rows = bq_service.list_kpi_catalog(dataset_id=DASH_DATASET, dataset_filter=datasetId, table_filter=tableId)
		return {"items": rows}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/cxo/start")
def cxo_start(payload: Dict[str, Any]):
	try:
		dashboard_id = payload.get('dashboard_id') or ''
		dashboard_name = payload.get('dashboard_name') or ''
		active_tab = payload.get('active_tab') or 'overview'
		conv_id = bq_service.create_cxo_conversation(dashboard_id, dashboard_name, active_tab, cxo_name="Naveen Alapati", cxo_title="CEO")
		return {"conversation_id": conv_id}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.post("/api/cxo/send")
def cxo_send(payload: Dict[str, Any]):
	try:
		conversation_id = payload.get('conversation_id')
		message = payload.get('message') or ''
		context = payload.get('context') or {}
		if not conversation_id:
			raise HTTPException(status_code=400, detail="conversation_id required")
		# store user message with embedding
		user_emb = None
		try:
			user_emb = embedding_service.embed_text(message)
		except Exception:
			user_emb = []
		bq_service.add_cxo_message(conversation_id, role="user", content=message, embedding=user_emb)
		# recent history (last 30 days)
		history = bq_service.list_cxo_messages(conversation_id, days=30)
		# build prompt from context (KPIs data summaries) and user message
		kpis = context.get('kpis') or []
		active_tab = context.get('active_tab') or 'overview'
		dashboard_name = context.get('dashboard_name') or ''
		# Aggregate KPI data (capped)
		kpis_with_data = []
		for k in kpis:
			rows = k.get('rows') or []
			if rows:
				kpis_with_data.append({"id": k.get('id'), "name": k.get('name'), "rows": rows[:100]})
		if not kpis_with_data:
			resp_md = "No data is available for the current tab. Run or refresh KPIs to generate a summary."
			bq_service.add_cxo_message(conversation_id, role="assistant", content=resp_md, embedding=[])
			return {"reply": resp_md}
		# System and user directives
		sys = (
			"You are CXO AI Assist for a CEO named Naveen Alapati. Professional strategist tone. "
			"Use only the provided KPI data rows and recent chat history (last 30 days). Be interactive: "
			"- If user asks broadly (e.g., 'areas that need attention'), list top 2–3 options and ask which one to drill into. "
			"- If user says 'Pick one', choose the highest urgency/risk item. "
			"- Avoid repeating prior summaries; add incremental insights. "
			"- Provide 3–5 bullets max and propose next steps (owner, timeline). "
			"- If data is insufficient, ask a clarifying question before answering. "
			"Output strictly Markdown with clear headings and bullet lists. No JSON."
		)
		user_obj = {
			"instruction": (
				"Create a CXO-ready Markdown summary focused on THREE sections (omit any without sufficient data):\n\n"
				"1. Executive Calls to Action — 3 bullets max; each bullet should include owner, due date, and measurable outcome.\n"
				"2. Financial Bridge and Sensitivities — a short 'what moved the number' bridge note and 1–2 bullets on the most material sensitivities (e.g., conversion, price, mix).\n"
				"3. Risk and Compliance Watchlist — 2–3 bullets on the most urgent risks (operational, data/fraud, regulatory) with mitigation steps.\n\n"
				"Rules: Avoid repetition; keep to 3–5 bullets per section; be precise and action-oriented.\n"
				"End with a brief call-to-action line inviting the CXO to interact with CXO AI Assist for deeper insights and a next-step action plan, and include the dashboard link: https://analytics-kpi-poc-315425729064.asia-south1.run.app"
			),
			"dashboard": dashboard_name,
			"active_tab": active_tab,
			"kpis": kpis_with_data,
			"history": history[-20:],
			"question": message,
		}
		resp = llm_client.generate_json(
			"Return JSON with key 'text' only, value is Markdown answer per instructions.",
			json.dumps(user_obj),
		)
		bot_text = ""
		try:
			bot_text = resp.get('text') if isinstance(resp, dict) else ""
		except Exception:
			bot_text = ""
		if not bot_text:
			bot_text = "No summary could be generated."
		# store assistant message with embedding
		asst_emb = None
		try:
			asst_emb = embedding_service.embed_text(bot_text)
		except Exception:
			asst_emb = []
		bq_service.add_cxo_message(conversation_id, role="assistant", content=bot_text, embedding=asst_emb)
		return {"reply": bot_text}
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