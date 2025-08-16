from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os

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
)
from .diagnostics import run_self_test

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET_EMBED = os.getenv("BQ_EMBEDDINGS_DATASET", "analytics_poc")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
EMBEDDING_MODE = os.getenv("EMBEDDING_MODE", "bigquery")
CREATE_INDEX_THRESHOLD = int(os.getenv("CREATE_INDEX_THRESHOLD", "5000"))

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
        rows = bq_service.query_rows(req.sql)
        return {"rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/selftest")
def api_selftest(dataset: Optional[str] = None, limit_tables: int = 3, sample_rows: int = 3, k: int = 3, run_kpis_limit: int = 2, force_llm: bool = True):
    try:
        report = run_self_test(bq_service, kpi_service, dataset=dataset, limit_tables=limit_tables, sample_rows=sample_rows, kpis_k=k, run_kpis_limit=run_kpis_limit, force_llm=force_llm)
        return report
    except Exception as exc:
        return {"error": str(exc)}

# Serve built SPA (Dockerfile copies frontend/dist to /app/static)
static_dir = os.path.abspath(os.getenv("STATIC_DIR", "/app/static"))
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")