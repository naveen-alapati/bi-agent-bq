# Analytics KPI POC

Single-page app that lists BigQuery datasets/tables, prepares embeddings, generates Top KPIs with SQL, and renders results with d3.js.

## Tech
- Backend: FastAPI (Python 3.11) on Cloud Run
- BigQuery: metadata + embeddings storage + vector index
- Embeddings: BigQuery ML via ML.GENERATE_EMBEDDING (preferred) or Vertex/OpenAI
- Frontend: React + Vite + d3

## Env Vars
- PROJECT_ID: your GCP project id
- BQ_LOCATION: e.g. US
- BQ_EMBEDDINGS_DATASET: e.g. analytics_poc
- EMBEDDING_MODE: bigquery | vertex | openai (default: bigquery)
- BQ_EMBEDDING_MODEL_FQN: required if EMBEDDING_MODE=bigquery, e.g. `project.dataset.embedding_model`
- VERTEX_LOCATION: region for Vertex, e.g. us-central1
- VERTEX_EMBEDDING_MODEL: textembedding-gecko@003 (default)
- LLM_PROVIDER: vertex | openai (default: vertex)
- VERTEX_LLM_MODEL: gemini-1.5-pro-001 (default)
- OPENAI_API_KEY: if using OpenAI
- OPENAI_EMBEDDING_MODEL: text-embedding-3-large
- OPENAI_LLM_MODEL: gpt-4o-mini
- CREATE_INDEX_THRESHOLD: default 5000

## BigQuery Setup
Create embeddings dataset and table is auto-created by backend. For BigQuery ML embeddings:
1) Create a remote model that references Vertex embeddings model:

```sql
CREATE OR REPLACE MODEL `PROJECT_ID.analytics_poc.embedding_model`
REMOTE WITH CONNECTION `REGION.connection_name`
OPTIONS (remote_service_type = 'CLOUD_AI_TEXT_EMBEDDING');
```

Then set `BQ_EMBEDDING_MODEL_FQN=PROJECT_ID.analytics_poc.embedding_model`.

## Local Dev
1) Backend
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PROJECT_ID=YOUR_PROJECT
export BQ_LOCATION=US
export BQ_EMBEDDINGS_DATASET=analytics_poc
export EMBEDDING_MODE=bigquery
export BQ_EMBEDDING_MODEL_FQN=PROJECT_ID.analytics_poc.embedding_model
uvicorn app.main:app --reload --port 8080
```

2) Frontend
```bash
cd frontend
npm i
npm run dev
```
Open http://localhost:5173

## Build SPA and run together
```bash
cd frontend && npm run build && cd ..
cd backend && uvicorn app.main:app --port 8080
```

## Docker & Cloud Run
```bash
# Build (from repo root)
docker build -t gcr.io/$PROJECT_ID/analytics-kpi-poc:latest -f backend/Dockerfile .
# Push
gcloud auth configure-docker
docker push gcr.io/$PROJECT_ID/analytics-kpi-poc:latest
# Deploy
gcloud run deploy analytics-kpi-poc \
  --image gcr.io/$PROJECT_ID/analytics-kpi-poc:latest \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=$PROJECT_ID,BQ_LOCATION=US,BQ_EMBEDDINGS_DATASET=analytics_poc,EMBEDDING_MODE=bigquery,BQ_EMBEDDING_MODEL_FQN=$PROJECT_ID.analytics_poc.embedding_model
```

Grant the Cloud Run service account BigQuery roles: `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor`.

## API
- GET /api/datasets
- GET /api/datasets/{datasetId}/tables
- POST /api/prepare {tables:[{datasetId,tableId}], sampleRows}
- POST /api/generate_kpis {tables:[...], k}
- POST /api/run_kpi {sql}
- GET /api/health

## Notes
- SQL from LLM must return d3-ready columns: timeseries -> x,y; categorical -> label,value; scatter -> x,y,(label).
- Vector index is created when embeddings table grows beyond threshold.