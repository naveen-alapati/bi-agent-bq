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
		params = []
		where_clauses = []
		# apply date filter if provided
		if req.date_column and req.filters and req.filters.get('date'):
			date_filter = req.filters['date']
			if date_filter.get('from'):
				where_clauses.append(f"{req.date_column} >= @fromDate")
				params.append(BigQueryService.bigquery.ScalarQueryParameter("fromDate", "DATE", date_filter['from']))
			if date_filter.get('to'):
				where_clauses.append(f"{req.date_column} <= @toDate")
				params.append(BigQueryService.bigquery.ScalarQueryParameter("toDate", "DATE", date_filter['to']))
		# apply categorical cross-filter if provided
		if req.filters and req.filters.get('category') and isinstance(req.filters['category'], dict):
			cat = req.filters['category']
			col = cat.get('column')
			val = cat.get('value')
			if col and val is not None:
				where_clauses.append(f"{col} = @catValue")
				params.append(BigQueryService.bigquery.ScalarQueryParameter("catValue", "STRING", str(val)))
		if where_clauses:
			wrapped = f"SELECT * FROM ( {sql} ) WHERE " + " AND ".join(where_clauses)
			rows = bq_service.client.query(wrapped, job_config=BigQueryService.bigquery.QueryJobConfig(query_parameters=params), location=bq_service.location)
			result_rows = [dict(r) for r in rows]
			return {"rows": bq_service._normalize_value(result_rows)}
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
		new_id = bq_service.save_dashboard(
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
		)
		return {"id": new_id, "name": req.name, "version": req.version or ""}
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


# Serve built SPA (Dockerfile copies frontend/dist to /app/static)
static_dir = os.path.abspath(os.getenv("STATIC_DIR", "/app/static"))
if os.path.isdir(static_dir):
	app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")