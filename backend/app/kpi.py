from typing import List, Dict, Any, Tuple
import json
import os
import uuid
from datetime import datetime, timezone

from .bq import BigQueryService
from .embeddings import EmbeddingService
from .models import TableRef, PreparedTable, KPIItem
from .llm import LLMClient


SYSTEM_PROMPT_TEMPLATE = (
    "You are a data analyst that only outputs JSON (no commentary). Use BigQuery SQL standard dialect. "
    "The user wants the Top {k} KPIs for the dataset/table(s) described below. For each KPI produce: "
    "- id (short slug), - name, - short_description (1 sentence), - chart_type (one of: line, bar, pie, area, scatter), "
    "- d3_chart (a short suggestion: e.g. \"d3.line() with x=date, y=value\", or \"d3.bar() with label,value\"), "
    "- expected_schema (one of: timeseries {{x:DATE|TIMESTAMP or STRING, y:NUMBER}}, categorical {{label:STRING, value:NUMBER}}, distribution {{label, value}}), "
    "- sql (BigQuery standard SQL) — this SQL must be ready-to-run and must return columns that match expected_schema. "
    "- Prefer engine='vega-lite' and provide vega_lite_spec that uses data: {{values: []}} and encodes fields x/y or label/value accordingly. "
    "Use the table reference exactly as `project.dataset.table`. If using aggregation, alias columns exactly to x,y or label,value depending on expected_schema. "
    "Keep SQL simple and efficient (use LIMIT where useful). Use safe handling for NULLs. "
    "INPUT_DATA is a JSON object. "
    "Return value: JSON object: {{ \"kpis\": [ {{id, name, short_description, chart_type, d3_chart, expected_schema, sql, engine?, vega_lite_spec? }} , ... ] }}"
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
        self.kpi_fallback_enabled = os.getenv("KPI_FALLBACK_ENABLED", "false").lower() == "true"

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
            content_rows.append(("table_summary", t.datasetId, t.tableId, "summary", content))
            for idx, row in enumerate(samples):
                content_rows.append(("sample_row", t.datasetId, t.tableId, f"row_{idx}", f"{row}"))

        table_fqn = f"{self.project_id}.{self.embedding_dataset}.table_embeddings"
        inserted = 0
        try:
            inserted = self.embeddings.generate_and_store_embeddings(self.bq, content_rows)
        except Exception as emb_exc:
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
                raise emb_exc

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
        # Deprecated for prod; kept behind flag for debugging
        return []

    def generate_kpis(self, tables: List[TableRef], k: int = 5) -> List[KPIItem]:
        all_items: List[KPIItem] = []
        for t in tables:
            try:
                system_prompt = SYSTEM_PROMPT_TEMPLATE.format(k=k)
                user_prompt = self._build_input_json([t])
                result = self.llm.generate_json(system_prompt, user_prompt)
            except Exception as exc:
                if self.kpi_fallback_enabled:
                    all_items.extend(self._fallback_kpis_for_table(t.datasetId, t.tableId, k))
                    continue
                print(f"KPI LLM error for {t.datasetId}.{t.tableId}: {exc}")
                continue
            table_slug = f"{t.datasetId}.{t.tableId}"
            # Attempt to infer a reasonable date column from schema for filtering
            date_col = None
            try:
                schema = self.bq.get_table_schema(t.datasetId, t.tableId)
                for c in schema:
                    if c.get('type') in ('DATE','TIMESTAMP','DATETIME'):
                        date_col = c['name']
                        break
            except Exception:
                pass
            count = 0
            for item in (result.get("kpis") or []):
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
                        engine=item.get("engine", "vega-lite" if item.get("vega_lite_spec") else "vega-lite"),
                        vega_lite_spec=item.get("vega_lite_spec"),
                        filter_date_column=item.get("filter_date_column") or date_col,
                    )
                )
                count += 1
        if not all_items:
            # Return empty list rather than raising, to avoid 500 and let UI handle gracefully
            return []
        return all_items

    def generate_custom_kpi(self, tables: List[TableRef], description: str, answers: List[str] = None) -> KPIItem:
        """
        Generate a custom KPI based on user description and clarifying questions.
        """
        try:
            # Build context from tables
            table_info = self._build_input_json(tables)
            
            # Create system prompt for custom KPI generation
            system_prompt = (
                "You are a data analyst that generates custom KPIs based on user requirements. "
                "You must output valid JSON with the following structure:\n"
                "{\n"
                "  \"clarifying_questions\": [\"question1\", \"question2\", ...],\n"
                "  \"kpi\": {\n"
                "    \"id\": \"custom_kpi\",\n"
                "    \"name\": \"KPI Name\",\n"
                "    \"short_description\": \"Brief description\",\n"
                "    \"chart_type\": \"line|bar|pie|area|scatter\",\n"
                "    \"expected_schema\": \"timeseries|categorical|distribution\",\n"
                "    \"sql\": \"BigQuery SQL query\",\n"
                "    \"engine\": \"vega-lite\",\n"
                "    \"vega_lite_spec\": {vega-lite specification},\n"
                "    \"filter_date_column\": \"date_column_name\"\n"
                "  }\n"
                "}\n\n"
                "If the user hasn't provided answers to clarifying questions yet, only return the questions. "
                "Once you have all the information needed, generate the complete KPI with SQL and chart specification. "
                "Use BigQuery standard SQL dialect. The SQL must return columns that match the expected_schema. "
                "For timeseries: columns should be x (DATE/TIMESTAMP) and y (NUMBER). "
                "For categorical: columns should be label (STRING) and value (NUMBER). "
                "For distribution: columns should be label and value. "
                "Use table references exactly as `project.dataset.table`. "
                "Keep SQL simple and efficient. Use safe handling for NULLs."
            )
            
            # Build user prompt
            user_prompt = json.dumps({
                "tables": json.loads(table_info),
                "user_description": description,
                "clarifying_questions_asked": answers is not None,
                "answers_provided": answers or []
            })
            
            print(f"Generating custom KPI with provider: {self.llm.provider}")
            result = self.llm.generate_json(system_prompt, user_prompt)
            print(f"LLM response received: {type(result)}")
            
            if not isinstance(result, dict):
                print(f"Invalid LLM response type: {type(result)}, content: {result}")
                raise Exception("Invalid response format from LLM")
            
            # Check if we need to ask clarifying questions
            if "clarifying_questions" in result and result["clarifying_questions"]:
                print(f"Returning clarifying questions: {result['clarifying_questions']}")
                return {"clarifying_questions": result["clarifying_questions"]}
            
            # Generate the KPI
            kpi_data = result.get("kpi", {})
            if not kpi_data:
                print(f"No KPI data in response: {result}")
                raise Exception("No KPI data generated")
            
            print(f"KPI data generated: {kpi_data}")
            
            # Create table slug for the KPI ID
            table_slug = f"{tables[0].datasetId}.{tables[0].tableId}"
            
            # Infer date column from schema
            date_col = None
            try:
                schema = self.bq.get_table_schema(tables[0].datasetId, tables[0].tableId)
                for c in schema:
                    if c.get('type') in ('DATE','TIMESTAMP','DATETIME'):
                        date_col = c['name']
                        break
            except Exception as schema_exc:
                print(f"Could not infer date column from schema: {schema_exc}")
                pass
            
            return KPIItem(
                id=f"{table_slug}:custom_{uuid.uuid4().hex[:8]}",
                name=kpi_data.get("name", "Custom KPI"),
                short_description=kpi_data.get("short_description", description),
                chart_type=kpi_data.get("chart_type", "bar"),
                d3_chart=kpi_data.get("d3_chart", ""),
                expected_schema=kpi_data.get("expected_schema", "categorical"),
                sql=kpi_data.get("sql", ""),
                engine=kpi_data.get("engine", "vega-lite"),
                vega_lite_spec=kpi_data.get("vega_lite_spec"),
                filter_date_column=kpi_data.get("filter_date_column") or date_col,
            )
            
        except Exception as exc:
            print(f"Custom KPI generation error: {exc}")
            import traceback
            traceback.print_exc()
            
            # Return a fallback KPI
            try:
                table_slug = f"{tables[0].datasetId}.{tables[0].tableId}"
                # Use proper BigQuery table reference format
                table_ref = f"`{tables[0].projectId}.{tables[0].datasetId}.{tables[0].tableId}`"
                
                return KPIItem(
                    id=f"{table_slug}:custom_fallback_{uuid.uuid4().hex[:8]}",
                    name="Custom KPI",
                    short_description=description,
                    chart_type="bar",
                    d3_chart="",
                    expected_schema="categorical",
                    sql=f"SELECT 'Custom KPI' as label, 1 as value FROM {table_ref} LIMIT 1",
                    engine="vega-lite",
                    vega_lite_spec=None,
                    filter_date_column=None,
                )
            except Exception as fallback_exc:
                print(f"Fallback KPI generation also failed: {fallback_exc}")
                # Last resort - return minimal KPI
                return KPIItem(
                    id=f"fallback_{uuid.uuid4().hex[:8]}",
                    name="Custom KPI",
                    short_description=description,
                    chart_type="bar",
                    d3_chart="",
                    expected_schema="categorical",
                    sql="SELECT 'Custom KPI' as label, 1 as value",
                    engine="vega-lite",
                    vega_lite_spec=None,
                    filter_date_column=None,
                )