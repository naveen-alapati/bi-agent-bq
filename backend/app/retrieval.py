from __future__ import annotations
from typing import List, Dict, Any, Optional
import os

from google.cloud import bigquery

from .bq import BigQueryService
from .embeddings import EmbeddingService, EmbeddingMode


class RetrievalPlugin:
	def __init__(
		self,
		bq: BigQueryService,
		embeddings: EmbeddingService,
		project_id: Optional[str],
		dataset: str,
		table: str = "ai_edit_library",
		top_k: int = 5,
	) -> None:
		self.bq = bq
		self.embeddings = embeddings
		self.project_id = project_id
		self.dataset = dataset
		self.table = table
		self.top_k = top_k
		self.enabled_env = os.getenv("RETRIEVAL_PLUGIN_ENABLED", "false").lower() == "true"

	def is_enabled(self, request_header_value: Optional[str]) -> bool:
		if not self.enabled_env:
			return False
		if request_header_value is None:
			return True
		v = str(request_header_value).strip().lower()
		return v in ("1", "true", "on", "yes")

	def _table_fqn(self) -> str:
		return f"{self.project_id}.{self.dataset}.{self.table}"

	def _table_issues_fqn(self) -> str:
		return f"{self.project_id}.{self.dataset}.table_issue_embeddings"

	def retrieve(
		self,
		task_type: str,
		intent_text: str,
		dialect: Optional[str] = None,
		tables: Optional[List[str]] = None,
		top_k: Optional[int] = None,
	) -> Dict[str, Any]:
		"""
		Return examples and optional policy hints from prior AI edit interactions.
		Falls back gracefully when the library table or embeddings are unavailable.
		"""
		k = int(top_k or self.top_k)
		try:
			table_fqn = self._table_fqn()
			# Embed the intent for ANN search when supported; for BigQuery mode, we will use BQML inline
			query_vector: Optional[List[float]] = None
			use_bq_query_vector = False
			try:
				if self.embeddings.mode in (EmbeddingMode.vertex, EmbeddingMode.openai):
					query_vector = self.embeddings.embed_text(intent_text or "")
				else:
					use_bq_query_vector = True
			except Exception:
				query_vector = None

			params = [
				bigquery.ScalarQueryParameter("tt", "STRING", task_type),
				bigquery.ScalarQueryParameter("k", "INT64", k),
			]
			where_clauses = ["task_type = @tt", "(accepted IS NULL OR accepted = TRUE)"]
			if dialect:
				where_clauses.append("(dialect IS NULL OR dialect = @dialect)")
				params.append(bigquery.ScalarQueryParameter("dialect", "STRING", dialect))
			if tables:
				where_clauses.append("(ARRAY_LENGTH(@tables) = 0 OR EXISTS (SELECT 1 FROM UNNEST(tables_used) t WHERE t IN UNNEST(@tables)))")
				params.append(bigquery.ArrayQueryParameter("tables", "STRING", tables))

			if query_vector:
				params.append(bigquery.ArrayQueryParameter("qvec", "FLOAT64", query_vector))
				sql = (
					f"""
					SELECT id, task_type, dialect, intent, rationale,
					       sql_before, sql_after, chart_before, chart_after, kpi_before, kpi_after,
					       created_at,
					       SAFE_CAST(VECTOR_DISTANCE(embedding, @qvec) AS FLOAT64) AS distance
					FROM `{table_fqn}`
					WHERE {' AND '.join(where_clauses)}
					  AND embedding IS NOT NULL
					  AND ARRAY_LENGTH(embedding) > 0
					  AND VECTOR_SEARCH(embedding, @qvec, @k)
					ORDER BY distance
					LIMIT @k
					"""
				)
			elif use_bq_query_vector:
				sql = (
					f"""
					WITH q AS (
					  SELECT ML.GENERATE_EMBEDDING(MODEL `{os.getenv('BQ_EMBEDDING_MODEL_FQN','')}`, @qtext) AS qvec
					)
					SELECT id, task_type, dialect, intent, rationale,
					       sql_before, sql_after, chart_before, chart_after, kpi_before, kpi_after,
					       created_at,
					       SAFE_CAST(VECTOR_DISTANCE(embedding, q.qvec) AS FLOAT64) AS distance
					FROM `{table_fqn}`, q
					WHERE {' AND '.join(where_clauses)}
					  AND embedding IS NOT NULL
					  AND ARRAY_LENGTH(embedding) > 0
					  AND VECTOR_SEARCH(embedding, q.qvec, @k)
					ORDER BY distance
					LIMIT @k
					"""
				)
				params.append(bigquery.ScalarQueryParameter("qtext", "STRING", intent_text or ""))
			else:
				sql = (
					f"""
					SELECT id, task_type, dialect, intent, rationale,
					       sql_before, sql_after, chart_before, chart_after, kpi_before, kpi_after,
					       created_at,
					       NULL AS distance
					FROM `{table_fqn}`
					WHERE {' AND '.join(where_clauses)}
					ORDER BY created_at DESC
					LIMIT @k
					"""
				)

			loc = self.bq.location
			job = self.bq.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=loc)
			rows = list(job)
			examples: List[Dict[str, Any]] = []
			for r in rows:
				examples.append(
					{
						"id": r.get("id"),
						"task_type": r.get("task_type"),
						"dialect": r.get("dialect"),
						"intent": r.get("intent"),
						"rationale": r.get("rationale"),
						"sql_before": r.get("sql_before"),
						"sql_after": r.get("sql_after"),
						"chart_before": r.get("chart_before"),
						"chart_after": r.get("chart_after"),
						"kpi_before": r.get("kpi_before"),
						"kpi_after": r.get("kpi_after"),
						"distance": float(r.get("distance")) if r.get("distance") is not None else None,
					}
				)
			# Retrieve table-level issues (non-blocking)
			issues: List[str] = []
			try:
				if tables:
					issues_sql = (
						f"""
						SELECT content
						FROM `{self._table_issues_fqn()}`
						WHERE ARRAY_LENGTH(@tables) = 0 OR CONCAT(dataset_id,'.',table_id) IN UNNEST(@tables)
						ORDER BY created_at DESC
						LIMIT 20
						"""
					)
					issues_job = self.bq.client.query(
						issues_sql,
						job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("tables", "STRING", tables)]),
						location=self.bq.location,
					)
					for r in issues_job:
						issues.append(r.get("content") or "")
			except Exception:
				issues = []
			return {"examples": examples, "tableIssues": issues, "policyHints": []}
		except Exception:
			# Any failure yields empty results (non-blocking)
			return {"examples": [], "tableIssues": [], "policyHints": []}