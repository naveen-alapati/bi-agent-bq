from typing import List, Dict, Any, Optional, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict
import json
import os
import re
from datetime import date, datetime, time, timezone
from decimal import Decimal
import uuid
import re as _re


class BigQueryService:
    def __init__(self, project_id: Optional[str], location: str = "US") -> None:
        self.project_id = project_id
        self.location = os.getenv("BQ_LOCATION", location)
        self.client = bigquery.Client(project=project_id)
        self._dataset_location_cache: Dict[str, str] = {}

    def _get_dataset_location(self, dataset_id: str) -> Optional[str]:
        if not dataset_id:
            return None
        if dataset_id in self._dataset_location_cache:
            return self._dataset_location_cache[dataset_id]
        try:
            ds = self.client.get_dataset(f"{self.project_id}.{dataset_id}")
            loc = getattr(ds, "location", None) or self.location
            self._dataset_location_cache[dataset_id] = loc
            return loc
        except Exception:
            return None

    def _infer_location_from_sql(self, sql: str) -> Optional[str]:
        # Look for backticked fully-qualified table refs: `project.dataset.table`
        m = re.search(r"`([\w-]+)\.([\w$-]+)\.([\w$-]+)`", sql)
        if m:
            proj, ds, _ = m.groups()
            if proj == self.project_id:
                return self._get_dataset_location(ds)
        # Fallback: unquoted pattern project.dataset.table (very approximate)
        m2 = re.search(r"\b([\w-]+)\.([\w$-]+)\.([\w$-]+)\b", sql)
        if m2:
            proj, ds, _ = m2.groups()
            if proj == self.project_id:
                return self._get_dataset_location(ds)
        return None

    def _normalize_value(self, v: Any) -> Any:
        if isinstance(v, bytes):
            return v.decode("utf-8")
        if isinstance(v, (datetime, date, time)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, list):
            return [self._normalize_value(x) for x in v]
        if isinstance(v, dict):
            return {k: self._normalize_value(val) for k, val in v.items()}
        return v

    def list_datasets(self) -> List[Dict[str, Any]]:
        datasets = []
        for ds in self.client.list_datasets(project=self.project_id):
            datasets.append(
                {
                    "datasetId": ds.dataset_id,
                    "friendlyName": None,
                    "description": None,
                }
            )
        return datasets

    def list_tables(self, dataset_id: str) -> List[Dict[str, Any]]:
        tables_info: List[Dict[str, Any]] = []
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)
        for tbl_item in self.client.list_tables(dataset_ref):
            tables_info.append(
                {
                    "tableId": tbl_item.table_id,
                    "rowCount": None,
                    "created": None,
                    "lastModified": None,
                }
            )
        return tables_info

    def get_table_schema(self, dataset_id: str, table_id: str) -> List[Dict[str, Any]]:
        table_ref = self.client.get_table(f"{self.project_id}.{dataset_id}.{table_id}")
        return [{"name": f.name, "type": f.field_type} for f in table_ref.schema]

    def sample_rows(self, dataset_id: str, table_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT * FROM `{self.project_id}.{dataset_id}.{table_id}`
        LIMIT {int(limit)}
        """
        job_config = bigquery.QueryJobConfig()
        loc = self._get_dataset_location(dataset_id) or self.location
        print(f"BQ QUERY location={loc} sql=SELECT * FROM `{self.project_id}.{dataset_id}.{table_id}` LIMIT {int(limit)}")
        query_job = self.client.query(sql, job_config=job_config, location=loc)
        rows: List[Dict[str, Any]] = []
        for row in query_job:
            row_dict = dict(row)
            rows.append({k: self._normalize_value(v) for k, v in row_dict.items()})
        return rows

    def query_rows(self, sql: str) -> List[Dict[str, Any]]:
        job_config = bigquery.QueryJobConfig()
        loc = self._infer_location_from_sql(sql) or self.location
        preview = sql.replace("\n", " ")
        if len(preview) > 400:
            preview = preview[:400] + "..."
        print(f"BQ QUERY location={loc} sql={preview}")
        query_job = self.client.query(sql, job_config=job_config, location=loc)
        results: List[Dict[str, Any]] = []
        for row in query_job:
            row_dict = dict(row)
            results.append({k: self._normalize_value(v) for k, v in row_dict.items()})
        return results

    def ensure_dataset(self, dataset_id: str) -> None:
        ds_ref = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        try:
            self.client.get_dataset(ds_ref)
        except NotFound:
            ds_ref.location = self.location
            self.client.create_dataset(ds_ref)

    def ensure_embeddings_table(self, dataset_id: str, table_name: str = "table_embeddings") -> str:
        self.ensure_dataset(dataset_id)
        table_id = f"{self.project_id}.{dataset_id}.{table_name}"
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("source_type", "STRING"),
            bigquery.SchemaField("dataset_id", "STRING"),
            bigquery.SchemaField("table_id", "STRING"),
            bigquery.SchemaField("object_ref", "STRING"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        try:
            self.client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
        return table_id

    def insert_embeddings_json(self, table_fqn: str, rows: List[Dict[str, Any]]) -> None:
        errors = self.client.insert_rows_json(table_fqn, rows)
        if errors:
            raise RuntimeError(f"Failed to insert embeddings: {errors}")

    def count_rows(self, table_fqn: str) -> int:
        sql = f"SELECT COUNT(*) as c FROM `{table_fqn}`"
        res = list(self.client.query(sql, location=self.location))
        return int(res[0]["c"]) if res else 0

    def create_vector_index_if_needed(self, table_fqn: str, index_name: str = "idx_table_embeddings") -> Optional[str]:
        create_sql = f"""
        CREATE VECTOR INDEX `{index_name}`
        ON `{table_fqn}` (embedding)
        OPTIONS(index_type='IVF', distance_type='COSINE')
        """
        try:
            self.client.query(create_sql, location=self.location).result()
            return index_name
        except Conflict:
            return index_name
        except Exception:
            return None

    def run_embedding_insert_with_bqml(self, embedding_model_fqn: str, target_table_fqn: str, content_rows: List[Tuple[str, str, str, str]]) -> int:
        if not content_rows:
            return 0
        selects: List[str] = []
        for idx, (source_type, dataset_id, table_id, object_ref) in enumerate(
            [(r[0], r[1], r[2], r[3]) for r in content_rows]
        ):
            content = content_rows[idx][4]
            esc_content = content.replace("'", "''")
            esc_source = source_type.replace("'", "''")
            esc_dataset = dataset_id.replace("'", "''")
            esc_table = table_id.replace("'", "''")
            esc_obj = object_ref.replace("'", "''")
            selects.append(
                f"SELECT '{esc_source}' AS source_type, '{esc_dataset}' AS dataset_id, '{esc_table}' AS table_id, '{esc_obj}' AS object_ref, '{esc_content}' AS content"
            )
        union_sql = " UNION ALL \n".join(selects)
        sql = f"""
        INSERT INTO `{target_table_fqn}` (id, source_type, dataset_id, table_id, object_ref, content, embedding, created_at)
        SELECT
          GENERATE_UUID() AS id,
          src.source_type,
          src.dataset_id,
          src.table_id,
          src.object_ref,
          src.content,
          ML.GENERATE_EMBEDDING(MODEL `{embedding_model_fqn}`, src.content) AS embedding,
          CURRENT_TIMESTAMP() AS created_at
        FROM (
          {union_sql}
        ) AS src
        """
        print(f"BQ QUERY location={self.location} sql=INSERT INTO `{target_table_fqn}` ...")
        self.client.query(sql, location=self.location).result()
        return 0

    def vector_search_topk_by_summary(self, embeddings_dataset: str, dataset_id: str, table_id: str, k: int = 10) -> List[Dict[str, Any]]:
        table_fqn = f"{self.project_id}.{embeddings_dataset}.table_embeddings"
        sql = f"""
        WITH q AS (
          SELECT embedding AS query_embedding
          FROM `{table_fqn}`
          WHERE dataset_id=@ds AND table_id=@tb AND source_type='table_summary'
          ORDER BY created_at DESC
          LIMIT 1
        )
        SELECT te.object_ref, te.content,
               VECTOR_DISTANCE(te.embedding, q.query_embedding) AS dist
        FROM `{table_fqn}` AS te, q
        WHERE te.dataset_id=@ds AND te.table_id=@tb
          AND VECTOR_SEARCH(te.embedding, q.query_embedding, @k)
        ORDER BY dist
        LIMIT @k
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ds", "STRING", dataset_id),
                bigquery.ScalarQueryParameter("tb", "STRING", table_id),
                bigquery.ScalarQueryParameter("k", "INT64", int(k)),
            ]
        )
        loc = self._get_dataset_location(embeddings_dataset) or self.location
        print(f"BQ QUERY location={loc} sql=VECTOR_SEARCH on {table_fqn}")
        results = self.client.query(sql, job_config=job_config, location=loc).result()
        return [
            {"object_ref": r["object_ref"], "content": r["content"], "dist": float(r["dist"]) if r["dist"] is not None else None}
            for r in results
        ]

    def vector_search_topk_by_query_vector(self, embeddings_dataset: str, query_vector: List[float], dataset_id: str, table_id: str, k: int = 10) -> List[Dict[str, Any]]:
        table_fqn = f"{self.project_id}.{embeddings_dataset}.table_embeddings"
        sql = f"""
        SELECT te.object_ref, te.content,
               VECTOR_DISTANCE(te.embedding, @qvec) AS dist
        FROM `{table_fqn}` AS te
        WHERE te.dataset_id=@ds AND te.table_id=@tb
          AND VECTOR_SEARCH(te.embedding, @qvec, @k)
        ORDER BY dist
        LIMIT @k
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("qvec", "FLOAT64", query_vector),
                bigquery.ScalarQueryParameter("ds", "STRING", dataset_id),
                bigquery.ScalarQueryParameter("tb", "STRING", table_id),
                bigquery.ScalarQueryParameter("k", "INT64", int(k)),
            ]
        )
        loc = self._get_dataset_location(embeddings_dataset) or self.location
        print(f"BQ QUERY location={loc} sql=VECTOR_SEARCH on {table_fqn}")
        results = self.client.query(sql, job_config=job_config, location=loc).result()
        return [
            {"object_ref": r["object_ref"], "content": r["content"], "dist": float(r["dist"]) if r["dist"] is not None else None}
            for r in results
        ]

    def _migrate_null_default_flags(self, table_fqn: str) -> None:
        """Migrate existing dashboards with NULL default_flag values to FALSE."""
        try:
            # Update any NULL default_flag values to FALSE
            sql = f"UPDATE `{table_fqn}` SET default_flag = FALSE WHERE default_flag IS NULL"
            self.client.query(sql, location=self.location).result()
        except Exception as e:
            print(f"Warning: Failed to migrate NULL default_flags: {e}")

    def ensure_dashboards_table(self, dataset_id: str = "analytics_dash", table: str = "dashboards") -> str:
        self.ensure_dataset(dataset_id)
        table_fqn = f"{self.project_id}.{dataset_id}.{table}"
        required = [
            ("id", "STRING"),
            ("name", "STRING"),
            ("version", "STRING"),
            ("kpis", "STRING"),
            ("layout", "STRING"),
            ("layouts", "STRING"),
            ("selected_tables", "STRING"),
            ("global_filters", "STRING"),
            ("theme", "STRING"),
            ("default_flag", "BOOL"),
            ("tabs", "STRING"),
            ("tab_layouts", "STRING"),
            ("last_active_tab", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
        ]
        try:
            table_obj = self.client.get_table(table_fqn)
            existing = {f.name: f.field_type for f in table_obj.schema}
            missing = [(n,t) for (n,t) in required if n not in existing]
            for name, typ in missing:
                ddl = f"ALTER TABLE `{table_fqn}` ADD COLUMN IF NOT EXISTS {name} {typ}"
                self.client.query(ddl, location=self.location).result()
            
            # Migrate existing NULL default_flags to FALSE
            self._migrate_null_default_flags(table_fqn)
        except NotFound:
            schema = [bigquery.SchemaField(n, t) for (n, t) in required]
            table_obj = bigquery.Table(table_fqn, schema=schema)
            self.client.create_table(table_obj)
        return table_fqn

    def set_default_dashboard(self, dashboard_id: str, dataset_id: str = "analytics_dash") -> None:
        table = self.ensure_dashboards_table(dataset_id)
        
        # First, validate that the dashboard exists
        sql = f"SELECT id FROM `{table}` WHERE id = @id LIMIT 1"
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id","STRING", dashboard_id)]), location=self.location)
        rows = list(job)
        if not rows:
            raise ValueError(f"Dashboard with ID '{dashboard_id}' not found")
        
        # Clear previous default and set new one
        # Use IS NOT DISTINCT FROM to handle NULL values properly
        sql = f"""
        UPDATE `{table}` SET default_flag = FALSE WHERE default_flag IS NOT DISTINCT FROM TRUE;
        UPDATE `{table}` SET default_flag = TRUE WHERE id = @id;
        """
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id","STRING", dashboard_id)]), location=self.location)
        list(job)

    def delete_dashboard(self, dashboard_id: str, dataset_id: str = "analytics_dash") -> None:
        """Delete a dashboard and ensure another becomes default if needed."""
        table = self.ensure_dashboards_table(dataset_id)
        
        # Check if the dashboard to be deleted is the default
        sql = f"SELECT default_flag FROM `{table}` WHERE id = @id"
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", dashboard_id)]),
            location=self.location,
        )
        rows = list(job)
        is_default = rows[0]["default_flag"] if rows else False
        
        # Delete the dashboard
        sql = f"DELETE FROM `{table}` WHERE id = @id"
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", dashboard_id)]),
            location=self.location,
        )
        job.result()
        
        # If the deleted dashboard was the default, ensure another becomes default
        if is_default:
            self._ensure_default_dashboard_exists(dataset_id)

    def clear_default_dashboards(self, dataset_id: str = "analytics_dash") -> None:
        """Clear all default flags from dashboards."""
        table = self.ensure_dashboards_table(dataset_id)
        sql = f"UPDATE `{table}` SET default_flag = FALSE WHERE default_flag IS NOT DISTINCT FROM TRUE"
        job = self.client.query(sql, location=self.location)
        list(job)

    def _ensure_default_dashboard_exists(self, dataset_id: str = "analytics_dash") -> None:
        """Ensure at least one dashboard is marked as default. If none exists, mark the most recent one as default."""
        table = self.ensure_dashboards_table(dataset_id)
        
        # Check if any dashboard has default_flag = TRUE
        sql = f"SELECT COUNT(*) as count FROM `{table}` WHERE default_flag = TRUE"
        rows = list(self.client.query(sql, location=self.location))
        has_default = rows[0]["count"] > 0 if rows else False
        
        if not has_default:
            # Find the most recently updated dashboard and mark it as default
            sql = f"SELECT id FROM `{table}` ORDER BY updated_at DESC LIMIT 1"
            rows = list(self.client.query(sql, location=self.location))
            if rows:
                dashboard_id = rows[0]["id"]
                # Set this dashboard as default
                sql = f"UPDATE `{table}` SET default_flag = TRUE WHERE id = @id"
                job = self.client.query(
                    sql, 
                    job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id","STRING", dashboard_id)]), 
                    location=self.location
                )
                list(job)

    def get_default_dashboard(self, dataset_id: str = "analytics_dash") -> Optional[str]:
        table = self.ensure_dashboards_table(dataset_id)
        
        # Ensure at least one dashboard is marked as default
        self._ensure_default_dashboard_exists(dataset_id)
        
        # First try to find a dashboard with default_flag = TRUE
        sql = f"SELECT id FROM `{table}` WHERE default_flag = TRUE LIMIT 1"
        rows = list(self.client.query(sql, location=self.location))
        if rows:
            return rows[0]["id"]
        
        # If no default is set, fallback to the most recently updated dashboard
        # This handles cases where default_flag is NULL or FALSE
        sql = f"SELECT id FROM `{table}` ORDER BY updated_at DESC LIMIT 1"
        rows = list(self.client.query(sql, location=self.location))
        return rows[0]["id"] if rows else None

    def _next_patch(self, current: Optional[str]) -> str:
        if not current:
            return "1.0.0"
        m = _re.match(r"(\d+)\.(\d+)\.(\d+)", current)
        if not m:
            return "1.0.0"
        maj, mi, pa = map(int, m.groups())
        return f"{maj}.{mi}.{pa+1}"

    def save_dashboard(self, name: str, kpis: List[Dict[str, Any]], layout: Optional[List[Dict[str, Any]]], layouts: Optional[Dict[str, List[Dict[str, Any]]]], selected_tables: List[Dict[str, Any]], global_filters: Optional[Dict[str, Any]], theme: Optional[Dict[str, Any]], version: Optional[str] = None, dashboard_id: Optional[str] = None, dataset_id: str = "analytics_dash", tabs: Optional[List[Dict[str, Any]]] = None, tab_layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None, last_active_tab: Optional[str] = None) -> Tuple[str, str]:
        table = self.ensure_dashboards_table(dataset_id)
        did = dashboard_id or uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        # determine next version if not provided: find latest for same name
        ver = version
        if not ver:
            try:
                rows = list(self.client.query(
                    f"SELECT version FROM `{table}` WHERE name=@n ORDER BY updated_at DESC LIMIT 1",
                    job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("n","STRING", name)]),
                    location=self.location,
                ))
                latest = dict(rows[0])['version'] if rows else None
                ver = self._next_patch(latest)
            except Exception:
                ver = "1.0.0"
        # Delete existing by id (Save) or same name+version (rare collision)
        try:
            self.client.query(
                f"DELETE FROM `{table}` WHERE id=@id OR (name=@n AND version=@v)",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("id","STRING", did),
                    bigquery.ScalarQueryParameter("n","STRING", name),
                    bigquery.ScalarQueryParameter("v","STRING", ver),
                ]),
                location=self.location,
            ).result()
        except Exception:
            pass
        row = {
            "id": did,
            "name": name,
            "version": ver,
            "kpis": json.dumps(kpis),
            "layout": json.dumps(layout or []),
            "layouts": json.dumps(layouts or {}),
            "selected_tables": json.dumps(selected_tables),
            "global_filters": json.dumps(global_filters or {}),
            "theme": json.dumps(theme or {}),
            "default_flag": False,  # Set default to FALSE for new dashboards
            "tabs": json.dumps(tabs or []),
            "tab_layouts": json.dumps(tab_layouts or {}),
            "last_active_tab": (last_active_tab or "overview"),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }
        errors = self.client.insert_rows_json(table, [row])
        if errors:
            print(f"Dashboard save errors: {errors}")
            raise RuntimeError(f"Failed to save dashboard: {errors}")
        return did, ver

    def list_dashboards(self, dataset_id: str = "analytics_dash") -> List[Dict[str, Any]]:
        table = self.ensure_dashboards_table(dataset_id)
        sql = f"SELECT id, name, version, default_flag, CAST(created_at AS STRING) AS created_at, CAST(updated_at AS STRING) AS updated_at FROM `{table}` ORDER BY name, updated_at DESC"
        rows = [dict(r) for r in self.client.query(sql, location=self.location)]
        return rows

    def get_dashboard(self, dashboard_id: str, dataset_id: str = "analytics_dash") -> Optional[Dict[str, Any]]:
        table = self.ensure_dashboards_table(dataset_id)
        sql = f"SELECT id, name, version, kpis, layout, layouts, selected_tables, global_filters, theme, default_flag, tabs, tab_layouts, last_active_tab, CAST(created_at AS STRING) AS created_at, CAST(updated_at AS STRING) AS updated_at FROM `{table}` WHERE id=@id LIMIT 1"
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", dashboard_id)]),
            location=self.location,
        )
        rows = list(job)
        if not rows:
            return None
        row = dict(rows[0])
        def parse_json_field(val: Any) -> Any:
            try:
                return json.loads(val)
            except Exception:
                return val
        return {
            "id": row["id"],
            "name": row["name"],
            "version": row.get("version"),
            "kpis": parse_json_field(row["kpis"]),
            "layout": parse_json_field(row["layout"]),
            "layouts": parse_json_field(row.get("layouts") or "{}"),
            "selected_tables": parse_json_field(row["selected_tables"]),
            "global_filters": parse_json_field(row.get("global_filters") or "{}"),
            "theme": parse_json_field(row.get("theme") or "{}"),
            "default_flag": row.get("default_flag", False),
            "tabs": parse_json_field(row.get("tabs") or "[]"),
            "tab_layouts": parse_json_field(row.get("tab_layouts") or "{}"),
            "last_active_tab": row.get("last_active_tab") or "overview",
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def ensure_kpi_catalog(self, dataset_id: str = "analytics_dash", table: str = "kpi_catalog") -> str:
        self.ensure_dataset(dataset_id)
        table_fqn = f"{self.project_id}.{dataset_id}.{table}"
        try:
            self.client.get_table(table_fqn)
        except NotFound:
            schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("sql", "STRING"),
                bigquery.SchemaField("chart_type", "STRING"),
                bigquery.SchemaField("expected_schema", "STRING"),
                bigquery.SchemaField("dataset_id", "STRING"),
                bigquery.SchemaField("table_id", "STRING"),
                bigquery.SchemaField("tags", "STRING"),
                bigquery.SchemaField("engine", "STRING"),
                bigquery.SchemaField("vega_lite_spec", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("usage_count", "INT64"),
            ]
            table_obj = bigquery.Table(table_fqn, schema=schema)
            self.client.create_table(table_obj)
        return table_fqn

    def add_to_kpi_catalog(self, items: List[Dict[str, Any]], dataset_id: str = "analytics_dash") -> int:
        table = self.ensure_kpi_catalog(dataset_id)
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for item in items:
            rows.append({
                "id": uuid.uuid4().hex,
                "name": item.get('name', ''),
                "sql": item.get('sql', ''),
                "chart_type": item.get('chart_type', ''),
                "expected_schema": item.get('expected_schema', ''),
                "dataset_id": item.get('dataset_id', ''),
                "table_id": item.get('table_id', ''),
                "tags": json.dumps(item.get('tags') or {}),
                "engine": item.get('engine'),
                "vega_lite_spec": json.dumps(item.get('vega_lite_spec') or {}),
                "created_at": now,
                "usage_count": 0,
            })
        errors = self.client.insert_rows_json(table, rows)
        if errors:
            print(f"KPI catalog insert errors: {errors}")
            raise RuntimeError(f"Failed to insert kpis: {errors}")
        return len(rows)

    def list_kpi_catalog(self, dataset_id: str = "analytics_dash", dataset_filter: Optional[str] = None, table_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        table = self.ensure_kpi_catalog(dataset_id)
        sql = f"SELECT id, name, sql, chart_type, expected_schema, dataset_id, table_id, tags, engine, vega_lite_spec, CAST(created_at AS STRING) AS created_at, usage_count FROM `{table}`"
        conds = []
        params = []
        if dataset_filter:
            conds.append("dataset_id = @ds")
            params.append(bigquery.ScalarQueryParameter("ds", "STRING", dataset_filter))
        if table_filter:
            conds.append("table_id = @tb")
            params.append(bigquery.ScalarQueryParameter("tb", "STRING", table_filter))
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        rows = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=self.location)
        out = []
        for r in rows:
            row = dict(r)
            row['tags'] = json.loads(row['tags']) if row.get('tags') else {}
            row['vega_lite_spec'] = json.loads(row['vega_lite_spec']) if row.get('vega_lite_spec') else None
            out.append(row)
        return out

    def ensure_cxo_tables(self, dataset_id: str = "analytics_cxo") -> Tuple[str, str]:
        self.ensure_dataset(dataset_id)
        conv_fqn = f"{self.project_id}.{dataset_id}.cxo_conversations"
        msg_fqn = f"{self.project_id}.{dataset_id}.cxo_messages"
        # conversations table
        try:
            self.client.get_table(conv_fqn)
        except NotFound:
            conv_schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("dashboard_id", "STRING"),
                bigquery.SchemaField("dashboard_name", "STRING"),
                bigquery.SchemaField("active_tab", "STRING"),
                bigquery.SchemaField("cxo_name", "STRING"),
                bigquery.SchemaField("cxo_title", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ]
            self.client.create_table(bigquery.Table(conv_fqn, schema=conv_schema))
        # messages table
        try:
            self.client.get_table(msg_fqn)
        except NotFound:
            msg_schema = [
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("conversation_id", "STRING"),
                bigquery.SchemaField("role", "STRING"),
                bigquery.SchemaField("content", "STRING"),
                bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
            ]
            self.client.create_table(bigquery.Table(msg_fqn, schema=msg_schema))
        return conv_fqn, msg_fqn

    def create_cxo_conversation(self, dashboard_id: str, dashboard_name: str, active_tab: str, cxo_name: str, cxo_title: str, dataset_id: str = "analytics_cxo") -> str:
        conv_fqn, _ = self.ensure_cxo_tables(dataset_id)
        from datetime import datetime, timezone
        import uuid
        conv_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "id": conv_id,
            "dashboard_id": dashboard_id,
            "dashboard_name": dashboard_name,
            "active_tab": active_tab,
            "cxo_name": cxo_name,
            "cxo_title": cxo_title,
            "created_at": now,
            "updated_at": now,
        }
        errors = self.client.insert_rows_json(conv_fqn, [row])
        if errors:
            raise RuntimeError(f"Failed to create conversation: {errors}")
        return conv_id

    def add_cxo_message(self, conversation_id: str, role: str, content: str, embedding: Optional[List[float]] = None, dataset_id: str = "analytics_cxo") -> str:
        _, msg_fqn = self.ensure_cxo_tables(dataset_id)
        from datetime import datetime, timezone
        import uuid
        msg_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "id": msg_id,
            "conversation_id": conversation_id,
            "role": role,
            "content": content,
            "embedding": embedding or [],
            "created_at": now,
        }
        errors = self.client.insert_rows_json(msg_fqn, [row])
        if errors:
            raise RuntimeError(f"Failed to insert message: {errors}")
        return msg_id

    def list_cxo_messages(self, conversation_id: str, days: int = 30, dataset_id: str = "analytics_cxo") -> List[Dict[str, Any]]:
        _, msg_fqn = self.ensure_cxo_tables(dataset_id)
        try:
            days_int = int(days)
        except Exception:
            days_int = 30
        sql = (
            f"SELECT role, content, CAST(created_at AS STRING) AS created_at "
            f"FROM `{msg_fqn}` "
            f"WHERE conversation_id=@cid AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_int} DAY) "
            f"ORDER BY created_at ASC"
        )
        rows = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("cid", "STRING", conversation_id)]), location=self.location)
        return [dict(r) for r in rows]