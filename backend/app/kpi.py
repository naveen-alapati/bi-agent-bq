from typing import List, Dict, Any, Tuple
import json
import os
import uuid
from datetime import datetime, timezone
import re

from .bq import BigQueryService
from .embeddings import EmbeddingService
from .models import TableRef, PreparedTable, KPIItem
from .llm import LLMClient


SYSTEM_PROMPT_TEMPLATE = (
    "You are a seasoned enterprise data analyst with 20 years of experience. Output JSON only (no commentary). "
    "Use BigQuery SQL (Standard SQL). The user wants the Top {k} high-impact KPIs for the dataset/table(s) described below. "
    "Your KPIs should be decision-grade (not vanity metrics) and help executives understand performance, growth, efficiency, and risk.\n\n"
    "Rules for each KPI you produce:\n"
    "- Provide fields: id (short slug), name, short_description (1 sentence), chart_type (line|bar|pie|area|scatter), d3_chart (short hint),\n"
    "  expected_schema (one of: timeseries {{x:DATE|TIMESTAMP or STRING, y:NUMBER}}, categorical {{label:STRING, value:NUMBER}}, distribution {{label, value}}),\n"
    "  sql (ready-to-run BigQuery SQL), engine ('vega-lite'), vega_lite_spec (valid spec with data:{{values: []}}), and filter_date_column when applicable.\n"
    "- The SQL MUST return columns aliased exactly as required by expected_schema: for timeseries use x,y; for categorical use label,value; for distribution use label,value.\n"
    "- Only reference columns that exist in the provided schema. Use exact column names (case-sensitive as listed). If a desired KPI is not feasible with the schema, skip it.\n"
    "- Prefer efficient queries. Use COALESCE to handle NULLs. Use LIMIT for categorical Top-N (e.g., 10). Avoid SELECT *.\n"
    "- Always use SAFE_DIVIDE(numerator, denominator) for any ratio or percentage; never use bare '/' division. This prevents division-by-zero errors.\n"
    "- If a date or timestamp column exists, include at least two time-series KPIs that show trend and growth.\n"
    "- If numeric measures exist, include growth/velocity (MoM/YoY) and rolling averages (e.g., 7d/28d).\n"
    "- If categorical dimensions exist, include contribution mix (Top-N by value) and concentration (share of top categories).\n"
    "- Where feasible, surface risk/quality (e.g., anomaly scores via z-score on daily totals) as a time-series y value.\n\n"
    "For distribution, produce a histogram-like bucketization or percentile summary as label/value pairs.\n\n"
    "Guidance for mapping schema to KPIs:\n"
    "- Identify one primary date column if present (name hints: date, dt, created_at, updated_at, timestamp). Use it as filter_date_column and timeseries x.\n"
    "- Identify numeric measure columns (names often include amount, revenue, cost, price, qty, count, score, duration).\n"
    "- Identify categorical dimensions (e.g., country, region, product, segment, channel, status).\n\n"
    "Examples of strong enterprise KPIs to consider (only if supported by schema):\n"
    "- Timeseries trend of total records or sum of a key numeric measure with 7d rolling average.\n"
    "- MoM or YoY growth of a key measure (e.g., revenue, count). Use same-period-last-year comparison when a year of data is present.\n"
    "- Top 10 categories by contribution (e.g., product, country) measured by sum(amount) or count(*).\n"
    "- Concentration metric: share of Top 3 categories vs total (as categorical rows).\n"
    "- Distribution of a numeric measure using buckets (e.g., price ranges) or selected percentiles as label/value pairs.\n"
    "- Anomaly indicator: daily z-score of total count or sum(amount) as y (higher absolute values indicate anomalies).\n\n"
    "Vega-Lite guidance:\n"
    "- timeseries: line or area chart with x mapped to 'x' (temporal when date/timestamp) and y to 'y'.\n"
    "- categorical: bar chart with x='label', y='value', sort by '-value'.\n"
    "- distribution: bar chart with x='label', y='value' or appropriate encoding.\n\n"
    "INPUT_DATA is a JSON object.\n"
    "Return value: JSON object: {{ \"kpis\": [ {{id, name, short_description, chart_type, d3_chart, expected_schema, sql, engine, vega_lite_spec, filter_date_column? }} , ... ] }}"
)

CROSS_SYSTEM_PROMPT_TEMPLATE = (
    "You are a seasoned enterprise data analyst. Output JSON only. Use BigQuery Standard SQL.\n\n"
    "Goal: Propose up to {k} high-impact cross-table KPIs using JOINS between the provided tables. Favor fact tables joined to dimension tables.\n\n"
    "Strict requirements:\n"
    "- Only use columns that exist in the provided schemas. Use exact, fully-qualified table references as `project.dataset.table`.\n"
    "- Only JOIN when a valid key exists in both tables (e.g., *_id, id, keys with matching semantics).\n"
    "- Ensure SQL aliases match expected_schema: timeseries -> x,y; categorical -> label,value; distribution -> label,value.\n"
    "- Prefer efficient aggregations; avoid SELECT *. Use COALESCE for NULLs.\n"
    "- Always use SAFE_DIVIDE(numerator, denominator) for ratios/percentages; never use bare '/' division.\n"
    "- If time columns exist, create trend and growth KPIs. Otherwise focus on categorical contribution and distributions.\n\n"
    "Examples to consider (only if schema supports them):\n"
    "- Revenue (or key measure) by product/category/region/channel (JOIN fact to dimension).\n"
    "- Conversion rate or average order value requiring measures and dimensional attributes.\n"
    "- Customer mix: Top 10 segments by contribution.\n"
    "- Retention/repurchase rate by cohort (if dates and customer_id exist).\n"
    "- On-time vs delayed (if status/dates exist).\n\n"
    "Output JSON shape: {{ \"kpis\": [ {{id, name, short_description, chart_type, d3_chart, expected_schema, sql, engine, vega_lite_spec, filter_date_column?}}, ... ] }}\n"
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
        issue_rows: List[Tuple[str, str, str]] = []  # (dataset_id, table_id, content)
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

            # Build compact table issue hints (heuristics)
            try:
                cols = {c.get('name',''): (c.get('type','') or '').upper() for c in (schema or [])}
                num_nulls_hint = ""
                # Leave placeholder hints; real null rates would come from profiling if available
                date_cols = [n for n,tpe in cols.items() if 'DATE' in tpe or 'TIMESTAMP' in tpe]
                bool_like = [n for n in cols if re.search(r"is_|_flag$|^flag_", n, re.I)]
                text_like = [n for n,tpe in cols.items() if tpe in ("STRING", "BYTES")]
                hints: List[str] = []
                if date_cols:
                    hints.append(f"partition_or_filter: {date_cols[0]}")
                if bool_like:
                    hints.append("booleans_may_be_strings")
                if text_like and any('amount' in n.lower() or 'price' in n.lower() for n in cols):
                    hints.append("unit_mismatch_check")
                # Always include safe ops guidance
                hints.append("prefer_SAFE_CAST_SAFE_DIVIDE")
                if hints:
                    issue_rows.append((t.datasetId, t.tableId, f"table: {t.datasetId}.{t.tableId} | issues: {', '.join(hints)}"))
            except Exception:
                pass

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

        # Insert table issue embeddings when using BQML mode, else skip silently
        try:
            if issue_rows and self.embeddings.mode == self.embeddings.mode.bigquery and self.embeddings.bqml_model_fqn:
                issues_table = self.bq.ensure_table_issue_embeddings(self.embedding_dataset, table="table_issue_embeddings")
                self.bq.insert_table_issue_embeddings_with_bqml(self.embeddings.bqml_model_fqn, issues_table, issue_rows)
        except Exception:
            pass

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

    def _score_table_for_primary(self, table: TableRef) -> int:
        score = 0
        name = table.tableId.lower()
        if "fact" in name or "fct" in name:
            score += 10
        if name.startswith("dim_") or name.startswith("dim") or name.startswith("d_"):
            score -= 3
        try:
            schema = self.bq.get_table_schema(table.datasetId, table.tableId)
        except Exception:
            schema = []
        numeric_types = {"INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
        num_numeric_cols = sum(1 for c in schema if (c.get("type") or "").upper() in numeric_types)
        score += min(5, num_numeric_cols)
        return score

    def _select_primary_table(self, tables: List[TableRef]) -> TableRef:
        if not tables:
            raise ValueError("No tables provided")
        return max(tables, key=self._score_table_for_primary)

    def _infer_date_col_from_schema(self, dataset_id: str, table_id: str) -> str:
        try:
            schema = self.bq.get_table_schema(dataset_id, table_id)
            for c in schema:
                if c.get('type') in ('DATE','TIMESTAMP','DATETIME'):
                    return c['name']
        except Exception:
            return None
        return None

    def _normalize_expected_schema(self, expected: Any) -> str:
        """Return a normalized expected_schema string from various shapes or dicts.

        Accepts flexible inputs (string variants, dicts with x/y or label/value) and maps
        to one of: timeseries | categorical | distribution | scatter. Returns empty string
        when it cannot be determined.
        """
        try:
            # Direct string handling with fuzzy mapping
            if isinstance(expected, str):
                s = expected.strip().lower()
                if not s:
                    return ""
                # Common fuzzy variants
                if "time" in s or "series" in s:
                    return "timeseries"
                for candidate in ("timeseries", "categorical", "distribution", "scatter"):
                    if s.startswith(candidate) or candidate in s:
                        return candidate
                return ""
            # Dict-like schema
            if isinstance(expected, dict):
                schema_type = expected.get("type")
                if isinstance(schema_type, str):
                    return self._normalize_expected_schema(schema_type)
                lowered_keys = {str(k).lower() for k in expected.keys()}
                if {"x", "y"}.issubset(lowered_keys):
                    return "timeseries"
                if {"label", "value"}.issubset(lowered_keys):
                    return "categorical"
                return ""
            return ""
        except Exception:
            return ""

    def _normalize_chart_type(self, chart_type: Any) -> str:
        """Normalize chart_type; pick first valid token from free-form strings."""
        valid = {"line", "bar", "pie", "area", "scatter"}
        try:
            if isinstance(chart_type, str):
                tokens = re.split(r"[^a-zA-Z]+", chart_type.strip().lower())
                for tok in tokens:
                    if tok in valid:
                        return tok
            return "bar"
        except Exception:
            return "bar"

    def _strip_code_fences(self, text: Any) -> str:
        """Remove triple backtick code fences and language hints from a string value."""
        if not isinstance(text, str):
            return ""
        s = text.strip()
        if s.startswith("```"):
            # remove leading and trailing fences
            s = s.strip("`")
            # drop leading language tag if present
            if s.startswith("json") or s.startswith("sql"):
                s = s.split("\n", 1)[1] if "\n" in s else ""
        return s.strip()

    def _normalize_vega_lite_spec(self, spec: Any) -> Any:
        """Ensure vega_lite_spec is a dict or None; parse JSON strings when possible."""
        try:
            if spec is None:
                return None
            if isinstance(spec, dict):
                # Ensure minimal skeleton for downstream renderers
                if "data" not in spec:
                    spec["data"] = {"values": []}
                elif isinstance(spec["data"], dict) and "values" not in spec["data"]:
                    spec["data"]["values"] = []
                return spec
            if isinstance(spec, str):
                s = self._strip_code_fences(spec)
                try:
                    obj = json.loads(s)
                    return self._normalize_vega_lite_spec(obj)
                except Exception:
                    return None
            return None
        except Exception:
            return None

    def _coerce_llm_result(self, result: Any) -> Dict[str, Any]:
        """Coerce LLM response into a dict with key 'kpis' when possible."""
        try:
            if isinstance(result, dict):
                return result
            if isinstance(result, str):
                s = self._strip_code_fences(result)
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        return obj
                    if isinstance(obj, list):
                        return {"kpis": obj}
                except Exception:
                    return {}
            if isinstance(result, list):
                return {"kpis": result}
            return {}
        except Exception:
            return {}

    def generate_kpis(self, tables: List[TableRef], k: int = 5, prefer_cross: bool = False) -> List[KPIItem]:
        table_items: List[KPIItem] = []
        cross_items: List[KPIItem] = []
        # Per-table KPIs (existing behavior, with lower budget when preferring cross)
        k_per_table = k
        if prefer_cross and len(tables) >= 2:
            # Keep per-table KPIs minimal when focusing on cross-table ideas
            k_per_table = max(1, min(k, 2))
        for t in tables:
            try:
                system_prompt = SYSTEM_PROMPT_TEMPLATE.format(k=k_per_table)
                user_prompt = self._build_input_json([t])
                result = self._coerce_llm_result(self.llm.generate_json(system_prompt, user_prompt))
            except Exception as exc:
                if self.kpi_fallback_enabled:
                    table_items.extend(self._fallback_kpis_for_table(t.datasetId, t.tableId, k_per_table))
                    continue
                print(f"KPI LLM error for {t.datasetId}.{t.tableId}: {exc}")
                continue
            table_slug = f"{t.datasetId}.{t.tableId}"
            # Attempt to infer a reasonable date column from schema for filtering
            date_col = self._infer_date_col_from_schema(t.datasetId, t.tableId)
            count = 0
            for raw in (result.get("kpis") or []):
                if count >= k_per_table:
                    break
                try:
                    sql = self._strip_code_fences(raw.get("sql", ""))
                    expected_schema = self._normalize_expected_schema(raw.get("expected_schema", ""))
                    if not sql or not expected_schema:
                        continue
                    slug = raw.get("id", f"kpi_{count+1}")
                    # Ensure timeseries can be filtered by date: default to 'x' which is the date alias
                    filter_col = raw.get("filter_date_column") or ("x" if isinstance(expected_schema, str) and expected_schema.startswith("timeseries") else date_col)
                    item = KPIItem(
                        id=f"{table_slug}:{slug}",
                        name=(raw.get("name") or "KPI"),
                        short_description=(raw.get("short_description") or ""),
                        chart_type=self._normalize_chart_type(raw.get("chart_type", "bar")),
                        d3_chart=(raw.get("d3_chart") or ""),
                        expected_schema=expected_schema,
                        sql=sql,
                        engine="vega-lite",
                        vega_lite_spec=self._normalize_vega_lite_spec(raw.get("vega_lite_spec")),
                        filter_date_column=filter_col,
                    )
                    table_items.append(item)
                    count += 1
                except Exception as item_exc:
                    print(f"Skipping malformed KPI for {table_slug}: {item_exc}")
        # Cross-table KPIs
        if len(tables) >= 2:
            try:
                primary = self._select_primary_table(tables)
                primary_slug = f"{primary.datasetId}.{primary.tableId}"
                system_prompt = CROSS_SYSTEM_PROMPT_TEMPLATE.format(k=max(1, min(k, 7)))
                user_prompt = self._build_input_json(tables)
                cross_result = self._coerce_llm_result(self.llm.generate_json(system_prompt, user_prompt))
                # Attempt to infer date column from primary; default to 'x' for timeseries
                primary_date_col = self._infer_date_col_from_schema(primary.datasetId, primary.tableId)
                count = 0
                for raw in (cross_result.get("kpis") or []):
                    if count >= k:
                        break
                    try:
                        sql = self._strip_code_fences(raw.get("sql", ""))
                        expected_schema = self._normalize_expected_schema(raw.get("expected_schema", ""))
                        if not sql or not expected_schema:
                            continue
                        base_slug = raw.get("id", f"cross_{count+1}")
                        slug = f"cross_{base_slug}"
                        filter_col = raw.get("filter_date_column") or ("x" if isinstance(expected_schema, str) and expected_schema.startswith("timeseries") else primary_date_col)
                        item = KPIItem(
                            id=f"{primary_slug}:{slug}",
                            name=(raw.get("name") or "KPI"),
                            short_description=(raw.get("short_description") or ""),
                            chart_type=self._normalize_chart_type(raw.get("chart_type", "bar")),
                            d3_chart=(raw.get("d3_chart") or ""),
                            expected_schema=expected_schema,
                            sql=sql,
                            engine="vega-lite",
                            vega_lite_spec=self._normalize_vega_lite_spec(raw.get("vega_lite_spec")),
                            filter_date_column=filter_col,
                        )
                        cross_items.append(item)
                        count += 1
                    except Exception as item_exc:
                        print(f"Skipping malformed cross-table KPI for {primary_slug}: {item_exc}")
            except Exception as exc:
                print(f"Cross-table KPI generation error: {exc}")
        combined = cross_items + table_items if (prefer_cross and len(tables) >= 2) else (table_items + cross_items)
        if not combined:
            # Return empty list rather than raising, to avoid 500 and let UI handle gracefully
            return []
        return combined

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
                "Keep SQL simple and efficient. Use safe handling for NULLs. Always use SAFE_DIVIDE(numerator, denominator) for ratios/percentages to avoid division-by-zero; never use bare '/' division."
            )
            
            # Build user prompt
            user_prompt = json.dumps({
                "tables": json.loads(table_info),
                "user_description": description,
                "clarifying_questions_asked": answers is not None,
                "answers_provided": answers or []
            })
            
            result = self.llm.generate_json(system_prompt, user_prompt)
            
            if not isinstance(result, dict):
                raise Exception("Invalid response format from LLM")
            
            # Check if we need to ask clarifying questions
            if "clarifying_questions" in result and result["clarifying_questions"]:
                return {"clarifying_questions": result["clarifying_questions"]}
            
            # Generate the KPI
            kpi_data = result.get("kpi", {})
            if not kpi_data:
                raise Exception("No KPI data generated")
            
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
            except Exception:
                pass
            
            return KPIItem(
                id=f"{table_slug}:custom_{uuid.uuid4().hex[:8]}",
                name=kpi_data.get("name", "Custom KPI"),
                short_description=kpi_data.get("short_description", description),
                chart_type=kpi_data.get("chart_type", "bar"),
                d3_chart=kpi_data.get("d3_chart", ""),
                expected_schema=self._normalize_expected_schema(kpi_data.get("expected_schema", "categorical")),
                sql=kpi_data.get("sql", ""),
                engine=kpi_data.get("engine", "vega-lite"),
                vega_lite_spec=kpi_data.get("vega_lite_spec"),
                filter_date_column=kpi_data.get("filter_date_column") or date_col,
            )
            
        except Exception as exc:
            print(f"Custom KPI generation error: {exc}")
            # Return a fallback KPI
            table_slug = f"{tables[0].datasetId}.{tables[0].tableId}"
            return KPIItem(
                id=f"{table_slug}:custom_fallback_{uuid.uuid4().hex[:8]}",
                name="Custom KPI",
                short_description=description,
                chart_type="bar",
                d3_chart="",
                expected_schema="categorical",
                sql=f"SELECT 'Custom KPI' as label, 1 as value FROM `{tables[0].datasetId}.{tables[0].tableId}` LIMIT 1",
                engine="vega-lite",
                vega_lite_spec=None,
                filter_date_column=None,
            )