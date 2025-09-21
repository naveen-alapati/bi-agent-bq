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
from fastapi import Request
import re
import uuid
from time import perf_counter

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
	AnalystChatRequest,
	AnalystChatResponse,
	ThoughtGraphSaveRequest,
	ThoughtGraphSaveResponse,
	ThoughtGraphListResponse,
	ThoughtGraphListItem,
	ThoughtGraphGetResponse,
	ThoughtGraphGenerateRequest,
	ThoughtGraphGenerateResponse,
)
from .diagnostics import run_self_test
from .llm import LLMClient
from .lineage import compute_lineage
from .retrieval import RetrievalPlugin

llm_client = LLMClient()

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET_EMBED = os.getenv("BQ_EMBEDDINGS_DATASET", "analytics_poc")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
EMBEDDING_MODE = os.getenv("EMBEDDING_MODE", "bigquery")
CREATE_INDEX_THRESHOLD = int(os.getenv("CREATE_INDEX_THRESHOLD", "5000"))
DASH_DATASET = os.getenv("DASHBOARDS_DATASET", "analytics_dash")
RETRIEVAL_TABLE = os.getenv("RETRIEVAL_TABLE", "ai_edit_library")
THOUGHT_DATASET = os.getenv("THOUGHT_GRAPHS_DATASET", "analytics_thought")

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
retrieval_plugin = RetrievalPlugin(
	bq=bq_service,
	embeddings=embedding_service,
	project_id=PROJECT_ID,
	dataset=BQ_DATASET_EMBED,
	table=RETRIEVAL_TABLE,
	top_k=5,
)
# Record accepted edit exemplars (SQL before/after, intent) into ai_edit_library
@app.post("/api/ai_edit/accept_example")
def ai_edit_accept_example(payload: Dict[str, Any]):
	try:
		# Required fields
		intent = payload.get('intent') or ''
		sql_before = payload.get('sql_before') or ''
		sql_after = payload.get('sql_after') or ''
		if not sql_after:
			raise HTTPException(status_code=400, detail="sql_after is required")
		# Optional
		task_type = payload.get('task_type') or 'KPI_UPDATE'
		dialect = payload.get('dialect') or 'bigquery'
		rationale = payload.get('rationale') or ''
		kpi_before = payload.get('kpi_before') or {}
		kpi_after = payload.get('kpi_after') or {}
		tables = payload.get('tables_used') or []
		# Ensure table exists
		lib_fqn = bq_service.ensure_ai_edit_library_table(BQ_DATASET_EMBED, table=RETRIEVAL_TABLE)
		# Insert with embedding per provider
		row = {
			"task_type": task_type,
			"dialect": dialect,
			"intent": intent,
			"rationale": rationale,
			"sql_before": sql_before,
			"sql_after": sql_after,
			"chart_before": "",
			"chart_after": "",
			"kpi_before": kpi_before,
			"kpi_after": kpi_after,
			"tables_used": tables,
		}
		if embedding_service.mode == EmbeddingMode.bigquery:
			model_fqn = embedding_service.bqml_model_fqn
			if not model_fqn:
				raise HTTPException(status_code=500, detail="BQ_EMBEDDING_MODEL_FQN not set for bigquery mode")
			bq_service.insert_ai_edit_library_row_with_bqml_embedding(lib_fqn, model_fqn, row)
		else:
			# External provider: precompute embedding client-side and store via insert_rows_json
			try:
				embed_text = (str(intent or '') + "\nSQL: " + str(sql_after or '')).strip()
				vec = embedding_service.embed_text(embed_text)
				now = datetime.utcnow().isoformat()
				json_row = {
					"id": uuid.uuid4().hex,
					"task_type": task_type,
					"dialect": dialect,
					"intent": intent,
					"rationale": rationale,
					"sql_before": sql_before,
					"sql_after": sql_after,
					"chart_before": "",
					"chart_after": "",
					"kpi_before": json.dumps(kpi_before)[:20000],
					"kpi_after": json.dumps(kpi_after)[:20000],
					"tables_used": tables,
					"accepted": True,
					"embedding": vec,
					"created_at": now,
				}
				bq_service.insert_ai_edit_library_rows(lib_fqn, [json_row])
			except Exception as exc:
				raise HTTPException(status_code=500, detail=str(exc))
		return {"status": "ok"}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


def _extract_table_refs(sql: str) -> List[str]:
	try:
		if not sql:
			return []
		refs: List[str] = []
		# Backticked fully-qualified refs
		for m in re.findall(r"`([\w-]+\.[\w$-]+\.[\w$-]+)`", sql):
			refs.append(m)
		# Unquoted (approximate); avoid duplicates
		for m in re.findall(r"\b([\w-]+\.[\w$-]+\.[\w$-]+)\b", sql):
			if m not in refs:
				refs.append(m)
		return refs
	except Exception:
		return []


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
		# Optionally enrich with thought graph context
		thought_graph = getattr(req, 'thought_graph', None)
		if not thought_graph and getattr(req, 'thought_graph_id', None):
			try:
				g = bq_service.get_thought_graph(req.thought_graph_id, dataset_id=THOUGHT_DATASET)
				thought_graph = g.get('graph') if g else None
			except Exception:
				thought_graph = None
		kpis = kpi_service.generate_kpis(req.tables, k=k, prefer_cross=bool(getattr(req, 'prefer_cross', False)), thought_graph=thought_graph)
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
		# Optional preview LIMIT wrapper (default 0 meaning disabled)
		preview_limit = int(getattr(req, 'preview_limit', 0) or 0)
		# Shape validation flag (default false)
		validate_shape = bool(getattr(req, 'validate_shape', False))
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
		# Helper to run query (with optional WHERE/LIMIT wrapper)
		def _run_query(q: str):
			final_sql = q
			if where_clauses:
				final_sql = f"SELECT * FROM ( {final_sql} ) WHERE " + " AND ".join(where_clauses)
			if preview_limit and preview_limit > 0:
				final_sql = f"SELECT * FROM ( {final_sql} ) LIMIT {int(preview_limit)}"
			job_config = bigquery.QueryJobConfig(query_parameters=params)
			rows_iter = bq_service.client.query(final_sql, job_config=job_config, location=bq_service.location)
			result_rows = [dict(r) for r in rows_iter]
			return [ { k: bq_service._normalize_value(v) for k, v in r.items() } for r in result_rows ]
		# First attempt
		start = perf_counter()
		try:
			rows = _run_query(sql)
			elapsed_ms = int((perf_counter()-start)*1000)
			# Optional schema/shape validation
			if validate_shape and req.expected_schema:
				viol = _validate_expected_shape(req.expected_schema, rows)
				if viol:
					# Surface as 400 to simplify client handling
					raise HTTPException(status_code=400, detail={"type": "ShapeMismatch", "message": viol, "sql": sql})
			return {"rows": rows}
		except Exception as inner_exc:
			msg = str(inner_exc).lower()
			# Add deterministic SAFE_DIVIDE rewrite as a fallback in addition to LLM edit
			def _rewrite_safe_divide(q: str) -> str:
				import re as _re
				pattern = r"(?<!SAFE_DIVIDE\()(?P<a>[A-Za-z0-9_\.\)\]]+)\s*/\s*(?P<b>[A-Za-z0-9_\.\(\[]+)"
				def _repl(m):
					return f"SAFE_DIVIDE({m.group('a')}, {m.group('b')})"
				return _re.sub(pattern, _repl, q, flags=_re.IGNORECASE)
			should_try_fix = ("divide by zero" in msg) or ("division by zero" in msg) or ("invalid" in msg and "/" in sql)
			if should_try_fix:
				try:
					fixed_sql = kpi_service.llm.edit_sql(sql, "Rewrite to use SAFE_DIVIDE for all divisions; preserve output columns and aliases.")
					rows = _run_query(fixed_sql)
					return {"rows": rows}
				except Exception:
					try:
						rows = _run_query(_rewrite_safe_divide(sql))
						return {"rows": rows}
					except Exception:
						pass
			raise inner_exc
	except HTTPException:
		raise
	except Exception as exc:
		# Return structured error payload to help frontend and AI Edit
		err_detail: Dict[str, Any] = {
			"type": exc.__class__.__name__,
			"message": str(exc),
			"rawMessage": repr(exc),
			"sql": sql,
		}
		try:
			errors = getattr(exc, "errors", None)
			if errors:
				err_detail["errors"] = errors
		except Exception:
			pass
		raise HTTPException(status_code=400, detail=err_detail)


def _validate_expected_shape(expected_schema: Optional[str], rows: List[Dict[str, Any]]) -> Optional[str]:
	try:
		esc = (expected_schema or '').strip().lower()
		if not esc:
			return None
		if esc.startswith('time'):
			if rows and (('x' not in rows[0]) or ('y' not in rows[0])):
				return "Expected timeseries columns x,y not found"
		elif esc.startswith('cat') or esc == 'categorical':
			if rows and (('label' not in rows[0]) or ('value' not in rows[0])):
				return "Expected categorical columns label,value not found"
		elif esc.startswith('dist'):
			if rows and (('label' not in rows[0]) or ('value' not in rows[0])):
				return "Expected distribution columns label,value not found"
		return None
	except Exception:
		return None


@app.post("/api/ai_edit/telemetry")
def ai_edit_telemetry(payload: Dict[str, Any]):
	try:
		table = bq_service.ensure_ai_edit_telemetry_table(BQ_DATASET_EMBED, table="ai_edit_telemetry")
		row = {
			"id": uuid.uuid4().hex,
			"kpi_id": payload.get('kpi_id') or '',
			"action": payload.get('action') or 'test',
			"success": bool(payload.get('success')),
			"runtime_ms": int(payload.get('runtime_ms') or 0),
			"row_count": int(payload.get('row_count') or 0),
			"attempt": int(payload.get('attempt') or 0),
			"error_type": payload.get('error_type') or '',
			"error_message": (payload.get('error_message') or '')[:1500],
			"dashboard_id": payload.get('dashboard_id') or '',
			"retrieval_enabled": bool(payload.get('retrieval_enabled')),
			"created_at": datetime.utcnow().isoformat(),
		}
		bq_service.insert_ai_edit_telemetry(table, [row])
		return {"status": "ok"}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/sql/edit")
def edit_sql(payload: Dict[str, str], request: Request):
	try:
		original_sql = payload.get('sql', '')
		instruction = payload.get('instruction', '')
		aug_instruction = instruction
		try:
			if retrieval_plugin.is_enabled(request.headers.get('X-Retrieval-Assist')):
				tables = _extract_table_refs(original_sql)
				ret = retrieval_plugin.retrieve(
					task_type="SQL_EDIT",
					intent_text=instruction or original_sql[:200],
					dialect="bigquery",
					tables=tables,
					top_k=3,
				)
				ex = ret.get("examples") or []
				issues = ret.get("tableIssues") or []
				lines: List[str] = []
				if issues:
					lines.append("Respect table-level constraints and known issues:")
					for it in issues[:5]:
						lines.append(f"- {it[:200]}")
				if ex:
					lines.append("Use prior accepted exemplars when rewriting. Examples:")
					for e in ex[:3]:
						intent = (e.get("intent") or "").strip()
						sql_after = (e.get("sql_after") or "").strip()
						if sql_after:
							lines.append(f"- Intent: {intent[:160]} | SQL: {sql_after[:400]}")
				if lines:
					aug_instruction = instruction + "\n\n" + "\n".join(lines)
		except Exception:
			pass
		new_sql = kpi_service.llm.edit_sql(original_sql, aug_instruction)
		return {"sql": new_sql}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))

@app.post("/api/kpi/edit")
def edit_kpi(payload: Dict[str, str], request: Request):
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
		# Optionally retrieve prior exemplars
		retrieval = None
		try:
			if retrieval_plugin.is_enabled(request.headers.get('X-Retrieval-Assist')):
				tables = _extract_table_refs(original_sql)
				retrieval = retrieval_plugin.retrieve(
					task_type="KPI_UPDATE",
					intent_text=instruction or (original_kpi.get('name') or ''),
					dialect="bigquery",
					tables=tables,
					top_k=3,
				)
		except Exception:
			retrieval = None
		user = json.dumps({
			"kpi": original_kpi,
			"sql": original_sql,
			"instruction": instruction,
			"retrieval_examples": (retrieval or {}).get("examples", []),
			"table_issues": (retrieval or {}).get("tableIssues", []),
		})
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
def edit_kpi_chat(payload: Dict[str, Any], request: Request):
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
		# Retrieval exemplars (optional)
		retrieval = None
		try:
			if retrieval_plugin.is_enabled(request.headers.get('X-Retrieval-Assist')):
				orig_sql = kpi.get('sql') or ''
				tables = _extract_table_refs(orig_sql)
				retrieval = retrieval_plugin.retrieve(
					task_type="KPI_UPDATE",
					intent_text=message or (kpi.get('name') or ''),
					dialect="bigquery",
					tables=tables,
					top_k=3,
				)
		except Exception:
			retrieval = None
		user = json.dumps({
			"kpi": kpi,
			"message": message,
			"history": history[-10:],
			"context": ctx,
			"retrieval_examples": (retrieval or {}).get("examples", []),
			"table_issues": (retrieval or {}).get("tableIssues", []),
		})
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
		# build prompt from context (KPIs + chart metadata) and user message
		kpis = context.get('kpis') or []
		active_tab = context.get('active_tab') or 'overview'
		dashboard_name = context.get('dashboard_name') or ''
		filters = context.get('filters') or {}
		# Aggregate KPI data (capped) and include metadata for precise reasoning
		kpis_with_data: List[Dict[str, Any]] = []
		for k in kpis:
			rows = k.get('rows') or []
			if rows:
				item: Dict[str, Any] = {
					"id": k.get('id'),
					"name": k.get('name'),
					"chart_type": k.get('chart_type'),
					"expected_schema": k.get('expected_schema'),
					"engine": k.get('engine'),
					"filter_date_column": k.get('filter_date_column'),
					"layout": k.get('layout'),
					"sql": (k.get('sql') or '')[:4000],
					"vega_lite_spec": k.get('vega_lite_spec'),
					"row_count": len(rows),
					"rows": rows,
				}
				kpis_with_data.append(item)
		if not kpis_with_data:
			resp_md = "No data is available for the current tab. Run or refresh KPIs to generate a summary."
			bq_service.add_cxo_message(conversation_id, role="assistant", content=resp_md, embedding=[])
			return {"reply": resp_md}
		# System and user directives
		sys = (
			"You are CXO AI Assist for a CEO named Naveen Alapati. Professional strategist tone. "
			"Use only the provided KPI data rows, chart metadata (chart_type, expected_schema, vega-lite spec), filters, and recent chat history (last 30 days). Be interactive: "
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
			"filters": filters,
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


@app.post("/api/analyst/chat", response_model=AnalystChatResponse)
def analyst_chat(req: AnalystChatRequest):
	"""
	Chat with an AI Analyst. Returns reply and optional KPI proposals.
	"""
	try:
		# Build system prompt focusing on cross-table value and guidance when insufficient data
		sys = (
			"You are a senior data analyst with 20 years of experience. Be practical and concise. "
			"Goal: generate high-value, actionable e-commerce KPIs with correct BigQuery Standard SQL. "
			"Use the provided tables, metadata, and sample records to infer entities (orders, items, customers, products, sessions, refunds, marketing). "
			"Prefer cross-table KPIs when possible (orders+items+customers+events+marketing). "
			"If joins are not possible, explicitly state which KPI(s) cannot be created and list the exact keys/dimensions that are missing. "
			"For each KPI ensure: correct grain, no double counting, safe NULL handling, and proper filter_date_column. "
			"Output JSON with keys: 'reply' (markdown summary, top KPIs, assumptions, and data gaps with missing keys per KPI) "
			"and optional 'kpis' (array of KPI objects). "
			"Each KPI object must include: id slug, name, short_description, chart_type, expected_schema, "
			"sql (BigQuery Standard SQL, no code fences), engine='vega-lite', vega_lite_spec, filter_date_column."
		)
		# Build table context (schema, samples, similar docs) from embeddings
		try:
			table_context = json.loads(kpi_service._build_input_json(req.tables))
		except Exception:
			table_context = {}
		user = {
			"message": req.message,
			"prefer_cross": bool(req.prefer_cross),
			"tables": [t.model_dump() if hasattr(t, 'model_dump') else dict(t) for t in req.tables],
			"table_context": table_context,
			"current_kpis": [k.model_dump() if hasattr(k, 'model_dump') else dict(k) for k in req.kpis],
			"history": [h.model_dump() if hasattr(h, 'model_dump') else dict(h) for h in (req.history or [])][-10:]
		}
		resp = llm_client.generate_json(sys, json.dumps(user))
		reply = ""
		kpi_props = None
		if isinstance(resp, dict):
			reply = resp.get('reply') or resp.get('markdown') or resp.get('text') or ""
			kp = resp.get('kpis')
			if isinstance(kp, list):
				# Normalize into KPIItem list using existing coercion helpers via KPIService
				kpi_props = []
				for raw in kp:
					try:
						sql = kpi_service._strip_code_fences(raw.get("sql", ""))
						expected_schema = kpi_service._normalize_expected_schema(raw.get("expected_schema", ""))
						if not sql or not expected_schema:
							continue
						# Use first table as base for id if not provided
						base = f"{req.tables[0].datasetId}.{req.tables[0].tableId}" if req.tables else "unknown.unknown"
						slug = raw.get("id", f"chat_{len(kpi_props)+1}")
						filter_col = raw.get("filter_date_column") or ("x" if isinstance(expected_schema, str) and expected_schema.startswith("timeseries") else None)
						item = {
							"id": f"{base}:{slug}",
							"name": raw.get("name") or "KPI",
							"short_description": raw.get("short_description") or "",
							"chart_type": kpi_service._normalize_chart_type(raw.get("chart_type", "bar")),
							"d3_chart": raw.get("d3_chart") or "",
							"expected_schema": expected_schema,
							"sql": sql,
							"engine": "vega-lite",
							"vega_lite_spec": kpi_service._normalize_vega_lite_spec(raw.get("vega_lite_spec")),
							"filter_date_column": filter_col,
						}
						kpi_props.append(item)
					except Exception:
						continue
		return {"reply": reply, "kpis": kpi_props}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


# Lineage API
@app.post("/api/lineage")
def lineage(payload: Dict[str, Any]):
	try:
		sql = payload.get('sql') or ''
		if not sql:
			raise HTTPException(status_code=400, detail="sql is required")
		dialect = payload.get('dialect') or 'bigquery'
		data = compute_lineage(sql, dialect=dialect, bq=bq_service)
		return data
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=400, detail={"type": exc.__class__.__name__, "message": str(exc)})

# ===== Thought Graph APIs =====
@app.get("/api/thought_graphs", response_model=ThoughtGraphListResponse)
def thought_graphs_list(datasetId: Optional[str] = None):
	try:
		rows = bq_service.list_thought_graphs(dataset_id=THOUGHT_DATASET, dataset_filter=datasetId)
		items = [ThoughtGraphListItem(**r) for r in rows]
		return {"graphs": items}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/thought_graphs/{graph_id}", response_model=ThoughtGraphGetResponse)
def thought_graphs_get(graph_id: str):
	try:
		row = bq_service.get_thought_graph(graph_id, dataset_id=THOUGHT_DATASET)
		if not row:
			raise HTTPException(status_code=404, detail="Not Found")
		# Coerce selected_tables into TableRef list
		selected = []
		for t in (row.get("selected_tables") or []):
			try:
				selected.append(TableRef(datasetId=t.get('datasetId'), tableId=t.get('tableId')))
			except Exception:
				continue
		return {
			"id": row.get("id"),
			"name": row.get("name"),
			"version": row.get("version"),
			"primary_dataset_id": row.get("primary_dataset_id"),
			"datasets": row.get("datasets"),
			"selected_tables": selected,
			"graph": row.get("graph"),
			"created_at": row.get("created_at"),
			"updated_at": row.get("updated_at"),
		}
	except HTTPException:
		raise
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/thought_graphs", response_model=ThoughtGraphSaveResponse)
def thought_graphs_save(req: ThoughtGraphSaveRequest):
	try:
		gid, ver = bq_service.save_thought_graph(
			name=req.name,
			selected_tables=[t.model_dump() if hasattr(t, 'model_dump') else dict(t) for t in req.selected_tables],
			graph=req.graph,
			datasets=req.datasets or [],
			primary_dataset_id=req.primary_dataset_id,
			version=None,
			graph_id=req.id,
			dataset_id=THOUGHT_DATASET,
		)
		return {"id": gid, "name": req.name, "version": ver}
	except Exception as exc:
		raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/thought_graph/generate", response_model=ThoughtGraphGenerateResponse)
def thought_graphs_generate(req: ThoughtGraphGenerateRequest):
	"""
	Generate an initial Thought Graph from selected tables using LLM. Returns graph JSON usable by UI.
	"""
	try:
		# Build a minimal input context from tables (schemas and sample rows) like KPIService
		try:
			context_json = kpi_service._build_input_json(req.tables)
		except Exception:
			context_json = "{}"
			sys = (
				"Auto-Generate KPI Thought Graph\n\n"
				"You are an AI-powered BI assistant that generates and maintains a KPI Thought Graph from business tables.\n"
				"Your job is to:\n\n"
				"Analyze the provided tables and their business domain.\n\n"
				"Generate a complete KPI Thought Graph with all relevant KPIs.\n\n"
				"Structure output in JSON node schema.\n\n"
				"Ensure KPIs can be used for automatic SQL generation in BigQuery.\n\n"
				"Rules for Graph Generation\n\n"
				"Node = KPI\n\n"
				"Only create nodes for KPIs, not raw columns.\n\n"
				"KPIs can be atomic (direct aggregations) or composite (derived from others).\n\n"
				"Graph Structure\n\n"
				"Build a directed acyclic graph (DAG).\n\n"
				"Leaf nodes (atomic KPIs): aggregations like Bookings, MRR, Invoices Amount, Billable Hours.\n\n"
				"Higher-level nodes (composite KPIs): formulas like Net New ARR, Win Rate, Gross Margin.\n\n"
				"KPI Node Schema\n"
				"Each KPI must strictly follow this structure:\n\n"
				"id: kpi_identifier\n"
				"name: Human Friendly KPI Name\n"
				"type: atomic | composite | ratio | window\n"
				"description: Business definition of KPI\n"
				"time_grain: DAY | WEEK | MONTH | QUARTER | YEAR\n"
				"dimensions: [list of dimension keys]\n"
				"children: [dependent KPI IDs if composite]\n"
				"formula: Expression using children\n"
				"sources:\n"
				"  - table: table_name\n"
				"    roles:\n"
				"      id: primary_key\n"
				"      measure: numeric_column\n"
				"      timestamp: date_column\n"
				"      dimension_keys: [dimension_columns]\n"
				"filters: [optional default WHERE clause]\n"
				"null_policy: treat_null_as_zero | strict_nulls\n"
				"currency_policy: if applicable\n"
				"tests:\n"
				"  - type: sanity_range | row_count_nonzero\n"
				"owners: [responsible_team]\n"
				"version: v1.0\n\n"
				"SQL Generation Guidance\n\n"
				"SQL dialect = BigQuery Standard SQL.\n\n"
				"Always generate atomic KPIs as CTEs, composites reference them.\n\n"
				"Use DATE_TRUNC for time-grain, COALESCE for nulls, SAFE_DIVIDE for ratios.\n\n"
				"System Task\n\n"
				"When given:\n\n"
				"A set of tables (with sample columns + business context).\n\n"
				"You must:\n\n"
				"Identify all relevant KPIs for those tables.\n\n"
				"Build the full KPI Thought Graph (atomic + composite).\n\n"
				"Return the output as a JSON graph definition with keys 'nodes' (array of KPI nodes following the schema) and 'edges' (array of dependencies with keys from and to using KPI IDs; use type 'DEPENDS_ON').\n\n"
				"Optionally, provide example SQLs for 2–3 KPIs to show how queries are generated under key 'examples' (array of {id, sql})."
			)
		user = json.dumps({
			"tables": json.loads(context_json),
			"prompt": req.prompt or "",
		})
		resp = llm_client.generate_json(sys, user)
			graph = {}
			if isinstance(resp, dict):
				# Accept flexible outputs and normalize to { nodes, edges }
				raw_nodes = None
				try:
					if isinstance(resp.get("nodes"), list):
						raw_nodes = resp.get("nodes")
					elif isinstance(resp.get("kpis"), list):
						raw_nodes = resp.get("kpis")
					elif isinstance(resp.get("graph"), dict) and isinstance(resp.get("graph", {}).get("nodes"), list):
						raw_nodes = resp.get("graph", {}).get("nodes")
				except Exception:
					raw_nodes = None
				nodes = []
				edges = []
				# Transform KPI nodes into visualization nodes
				if isinstance(raw_nodes, list):
					for k in raw_nodes:
						try:
							kid = None
							kname = None
							if isinstance(k, dict):
								kid = k.get("id")
								kname = (k.get("name") or k.get("label") or kid)
							if not kid:
								continue
							nodes.append({
								"id": kid,
								"type": "KPI",
								"label": kname,
								"props": {"kpi": k},
							})
							# Derive dependency edges from children
							children = k.get("children") if isinstance(k, dict) else None
							if isinstance(children, list):
								for ch in children:
									if ch:
										edges.append({"source": ch, "target": kid, "type": "DEPENDS_ON"})
						except Exception:
							continue
				# Also absorb explicit edges if provided
				try:
					if isinstance(resp.get("edges"), list):
						for e in resp.get("edges"):
							try:
								src = e.get("source") or e.get("from")
								tgt = e.get("target") or e.get("to")
								et = e.get("type") or "DEPENDS_ON"
								if src and tgt:
									edges.append({"source": src, "target": tgt, "type": et})
							except Exception:
								continue
				except Exception:
					pass
				graph = {"nodes": nodes, "edges": edges}
		# Fallback minimal graph of key columns only
			if not (isinstance(graph, dict) and isinstance(graph.get("nodes"), list) and graph["nodes"]):
				nodes = []
				for t in req.tables:
					# Minimal KPI placeholder per table
					kid = f"kpi_{t.tableId}_count"
					nodes.append({"id": kid, "type": "KPI", "label": f"{t.tableId} Count", "props": {"kpi": {"id": kid, "name": f"{t.tableId} Count", "type": "atomic", "description": f"Count of rows in {t.tableId}", "time_grain": "MONTH", "dimensions": [], "children": [], "formula": "", "sources": [{"table": f"{kpi_service.project_id}.{t.datasetId}.{t.tableId}", "roles": {"id": "id", "measure": "*", "timestamp": None, "dimension_keys": []}}], "null_policy": "treat_null_as_zero", "owners": ["analytics_team"], "version": "v1.0"}}})
				graph = {"nodes": nodes, "edges": []}
		name = req.name or (req.tables[0].tableId if req.tables else "Thought Graph")
		return {"graph": graph, "name": name}
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