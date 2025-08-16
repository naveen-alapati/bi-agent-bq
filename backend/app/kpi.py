from typing import List, Dict, Any, Tuple
import json
import os
import uuid
from datetime import datetime, timezone

from .bq import BigQueryService
from .embeddings import EmbeddingService
from .models import TableRef, PreparedTable, KPIItem
from .llm import LLMClient


SYSTEM_PROMPT = (
    "You are a data analyst that only outputs JSON (no commentary). Use BigQuery SQL standard dialect. "
    "The user wants the Top {k} KPIs for the dataset/table(s) described below. For each KPI produce: "
    "- id (short slug), - name, - short_description (1 sentence), - chart_type (one of: line, bar, pie, area, scatter), "
    "- d3_chart (a short suggestion: e.g. \"d3.line() with x=date, y=value\", or \"d3.bar() with label,value\"), "
    "- expected_schema (one of: timeseries {x:DATE|TIMESTAMP or STRING, y:NUMBER}, categorical {label:STRING, value:NUMBER}, distribution {label, value}), "
    "- sql (BigQuery standard SQL) — this SQL must be ready-to-run and must return columns that match expected_schema. "
    "Use the table reference exactly as `project.dataset.table`. If using aggregation, alias columns exactly to x,y or label,value depending on expected_schema. "
    "Keep SQL simple and efficient (use LIMIT where useful). Use safe handling for NULLs. "
    "Return value: JSON object: { \"kpis\": [ {id, name, short_description, chart_type, d3_chart, expected_schema, sql }, ... ] }"
)


class KPIService:
    def __init__(
        self,
        bq: BigQueryService,
        embeddings: EmbeddingService,
        project_id: str,
        embedding_dataset: str,
        create_index_threshold: int = 5000,
    ) -> None:
        self.bq = bq
        self.embeddings = embeddings
        self.project_id = project_id
        self.embedding_dataset = embedding_dataset
        self.create_index_threshold = create_index_threshold
        self.llm = LLMClient()

    def prepare_tables(self, tables: List[TableRef], sample_rows: int = 5) -> List[PreparedTable]:
        content_rows: List[Tuple[str, str, str, str, str]] = []
        for t in tables:
            try:
                schema = self.bq.get_table_schema(t.datasetId, t.tableId)
            except Exception:
                schema = []
            try:
                samples = self.bq.sample_rows(t.datasetId, t.tableId, limit=sample_rows)
            except Exception:
                samples = []
            content = self.embeddings.build_table_summary_content(self.project_id, t.datasetId, t.tableId, schema, samples)
            # table summary row
            content_rows.append(("table_summary", t.datasetId, t.tableId, "summary", content))
            # sample rows as separate docs
            for idx, row in enumerate(samples):
                content_rows.append(("sample_row", t.datasetId, t.tableId, f"row_{idx}", f"{row}"))

        table_fqn = f"{self.project_id}.{self.embedding_dataset}.table_embeddings"
        inserted = 0
        try:
            inserted = self.embeddings.generate_and_store_embeddings(self.bq, content_rows)
        except Exception as emb_exc:
            # Fallback: insert docs with empty embeddings to avoid hard failure
            try:
                table_fqn = self.bq.ensure_embeddings_table(self.embedding_dataset, table_name="table_embeddings")
                now_iso = datetime.now(timezone.utc).isoformat()
                json_rows = []
                for (source_type, ds, tb, obj, content) in content_rows:
                    json_rows.append(
                        {
                            "id": uuid.uuid4().hex,
                            "source_type": source_type,
                            "dataset_id": ds,
                            "table_id": tb,
                            "object_ref": obj,
                            "content": content,
                            "embedding": [],
                            "created_at": now_iso,
                        }
                    )
                self.bq.insert_embeddings_json(table_fqn, json_rows)
                inserted = len(json_rows)
            except Exception:
                # If even fallback fails, surface the original embedding error
                raise emb_exc

        count = 0
        try:
            count = self.bq.count_rows(table_fqn)
            if count >= self.create_index_threshold:
                self.bq.create_vector_index_if_needed(table_fqn)
        except Exception:
            pass

        prepared: List[PreparedTable] = []
        for t in tables:
            prepared.append(PreparedTable(datasetId=t.datasetId, tableId=t.tableId, embed_rows=inserted))
        return prepared

    def _build_input_json(self, tables: List[TableRef]) -> str:
        infos: List[Dict[str, Any]] = []
        for t in tables:
            try:
                schema = self.bq.get_table_schema(t.datasetId, t.tableId)
            except Exception:
                schema = []
            try:
                samples = self.bq.sample_rows(t.datasetId, t.tableId, limit=5)
            except Exception:
                samples = []
            # vector search nearest docs to give LLM extra context
            try:
                nearest = self.bq.vector_search_topk_by_summary(self.embedding_dataset, t.datasetId, t.tableId, k=10)
            except Exception:
                nearest = []
            infos.append(
                {
                    "project": self.project_id,
                    "dataset": t.datasetId,
                    "table": t.tableId,
                    "schema": schema,
                    "sample_rows": samples,
                    "similar_docs": nearest,
                    "notes": "",
                }
            )
        return json.dumps({"tables": infos})

    def _fallback_kpis_for_table(self, dataset_id: str, table_id: str, k: int) -> List[KPIItem]:
        """Generate simple, safe KPIs without LLM, based on schema heuristics."""
        try:
            schema = self.bq.get_table_schema(dataset_id, table_id)
        except Exception:
            schema = []
        project = self.project_id
        table_fqn = f"`{project}.{dataset_id}.{table_id}`"
        kpis: List[KPIItem] = []

        # Helpers to pick columns
        def find_col(types: List[str]) -> Any:
            for col in schema:
                if col.get("type", "").upper() in types:
                    return col["name"], col["type"].upper()
            return None, None

        # 1) Timeseries: monthly count by first date/time column
        date_name, date_type = find_col(["DATE", "TIMESTAMP", "DATETIME"])
        if date_name and len(kpis) < k:
            if date_type == "DATE":
                sql = (
                    f"SELECT FORMAT_DATE(\"%Y-%m\", DATE_TRUNC({date_name}, MONTH)) AS x, COUNT(*) AS y "
                    f"FROM {table_fqn} WHERE {date_name} IS NOT NULL GROUP BY x ORDER BY x"
                )
            elif date_type == "TIMESTAMP":
                sql = (
                    f"SELECT FORMAT_TIMESTAMP(\"%Y-%m\", TIMESTAMP_TRUNC({date_name}, MONTH)) AS x, COUNT(*) AS y "
                    f"FROM {table_fqn} WHERE {date_name} IS NOT NULL GROUP BY x ORDER BY x"
                )
            else:  # DATETIME
                sql = (
                    f"SELECT FORMAT_DATETIME(\"%Y-%m\", DATETIME_TRUNC({date_name}, MONTH)) AS x, COUNT(*) AS y "
                    f"FROM {table_fqn} WHERE {date_name} IS NOT NULL GROUP BY x ORDER BY x"
                )
            kpis.append(
                KPIItem(
                    id=f"{dataset_id}.{table_id}:ts_count",
                    name="Monthly row count",
                    short_description=f"Monthly counts by {date_name}",
                    chart_type="line",
                    d3_chart="d3.line() with x=month, y=count",
                    expected_schema="timeseries {x:STRING, y:NUMBER}",
                    sql=sql,
                )
            )

        # 2) Categorical: top categories by first STRING column
        str_name, _ = find_col(["STRING"])
        if str_name and len(kpis) < k:
            sql = (
                f"SELECT COALESCE({str_name}, 'UNKNOWN') AS label, COUNT(*) AS value "
                f"FROM {table_fqn} GROUP BY label ORDER BY value DESC LIMIT 10"
            )
            kpis.append(
                KPIItem(
                    id=f"{dataset_id}.{table_id}:top_{str_name}",
                    name=f"Top {str_name}",
                    short_description=f"Most frequent values of {str_name}",
                    chart_type="bar",
                    d3_chart="d3.bar() with label,value",
                    expected_schema="categorical {label:STRING, value:NUMBER}",
                    sql=sql,
                )
            )

        # 3) Numeric distribution by first numeric column
        num_name, _ = find_col(["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"])
        if num_name and len(kpis) < k:
            sql = (
                f"SELECT CAST(ROUND(CAST({num_name} AS FLOAT64), 0) AS STRING) AS label, COUNT(*) AS value "
                f"FROM {table_fqn} WHERE {num_name} IS NOT NULL GROUP BY label ORDER BY value DESC LIMIT 20"
            )
            kpis.append(
                KPIItem(
                    id=f"{dataset_id}.{table_id}:dist_{num_name}",
                    name=f"Distribution of {num_name}",
                    short_description=f"Count by rounded {num_name}",
                    chart_type="bar",
                    d3_chart="d3.bar() with label,value",
                    expected_schema="distribution {label:STRING, value:NUMBER}",
                    sql=sql,
                )
            )

        # 4) Scatter of first two numeric columns
        if len(kpis) < k:
            first_num, _ = find_col(["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"])
            second_num = None
            for col in schema:
                if col["name"] != first_num and col.get("type", "").upper() in ["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"]:
                    second_num = col["name"]
                    break
            if first_num and second_num and len(kpis) < k:
                sql = (
                    f"SELECT CAST({first_num} AS FLOAT64) AS x, CAST({second_num} AS FLOAT64) AS y "
                    f"FROM {table_fqn} WHERE {first_num} IS NOT NULL AND {second_num} IS NOT NULL LIMIT 500"
                )
                kpis.append(
                    KPIItem(
                        id=f"{dataset_id}.{table_id}:scatter_{first_num}_{second_num}",
                        name=f"Scatter: {first_num} vs {second_num}",
                        short_description=f"Sample of {first_num} vs {second_num}",
                        chart_type="scatter",
                        d3_chart="d3.scatter() with x,y",
                        expected_schema="scatter {x:NUMBER, y:NUMBER}",
                        sql=sql,
                    )
                )

        # 5) Total rows as single bar
        if len(kpis) < k:
            sql = f"SELECT 'rows' AS label, COUNT(*) AS value FROM {table_fqn}"
            kpis.append(
                KPIItem(
                    id=f"{dataset_id}.{table_id}:row_count",
                    name="Total rows",
                    short_description="Total row count",
                    chart_type="bar",
                    d3_chart="d3.bar() with label,value",
                    expected_schema="categorical {label:STRING, value:NUMBER}",
                    sql=sql,
                )
            )

        return kpis[:k]

    def generate_kpis(self, tables: List[TableRef], k: int = 5) -> List[KPIItem]:
        all_items: List[KPIItem] = []
        for t in tables:
            # Try LLM
            try:
                system_prompt = SYSTEM_PROMPT.format(k=k)
                user_prompt = self._build_input_json([t])
                result = self.llm.generate_json(system_prompt, user_prompt)
                table_slug = f"{t.datasetId}.{t.tableId}"
                count = 0
                for item in result.get("kpis", []):
                    if count >= k:
                        break
                    sql = item.get("sql", "")
                    expected_schema = item.get("expected_schema", "")
                    if not sql or not expected_schema:
                        continue
                    slug = item.get("id", f"kpi_{count+1}")
                    all_items.append(
                        KPIItem(
                            id=f"{table_slug}:{slug}",
                            name=item.get("name", "KPI"),
                            short_description=item.get("short_description", ""),
                            chart_type=item.get("chart_type", "bar"),
                            d3_chart=item.get("d3_chart", ""),
                            expected_schema=expected_schema,
                            sql=sql,
                        )
                    )
                    count += 1
                # If LLM returned too few, top up with fallback
                if count < k:
                    all_items.extend(self._fallback_kpis_for_table(t.datasetId, t.tableId, k - count))
            except Exception:
                # Fallback path
                all_items.extend(self._fallback_kpis_for_table(t.datasetId, t.tableId, k))
        return all_items