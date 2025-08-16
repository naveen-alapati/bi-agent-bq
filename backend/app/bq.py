from typing import List, Dict, Any, Optional, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict
import json
import os


class BigQueryService:
    def __init__(self, project_id: Optional[str], location: str = "US") -> None:
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)

    def list_datasets(self) -> List[Dict[str, Any]]:
        # Avoid get_dataset calls to minimize permissions; show ids only
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
        # Avoid get_table to reduce needed permissions; return ids only
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
        query_job = self.client.query(sql)
        rows = [dict(row) for row in query_job]
        return rows

    def query_rows(self, sql: str) -> List[Dict[str, Any]]:
        query_job = self.client.query(sql)
        results = [dict(row) for row in query_job]
        # Ensure JSON serializable: convert bytes to str etc
        normalized: List[Dict[str, Any]] = []
        for row in results:
            out: Dict[str, Any] = {}
            for k, v in row.items():
                if isinstance(v, bytes):
                    out[k] = v.decode("utf-8")
                else:
                    out[k] = v
            normalized.append(out)
        return normalized

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
        res = list(self.client.query(sql))
        return int(res[0]["c"]) if res else 0

    def create_vector_index_if_needed(self, table_fqn: str, index_name: str = "idx_table_embeddings") -> Optional[str]:
        # Best-effort create. Some locations may not yet support VECTOR INDEX metadata listing via INFORMATION_SCHEMA.
        create_sql = f"""
        CREATE VECTOR INDEX `{index_name}`
        ON `{table_fqn}` (embedding)
        OPTIONS(index_type='IVF', distance_type='COSINE')
        """
        try:
            self.client.query(create_sql).result()
            return index_name
        except Conflict:
            return index_name
        except Exception:
            # Ignore if not supported or already exists
            return None

    def run_embedding_insert_with_bqml(self, embedding_model_fqn: str, target_table_fqn: str, content_rows: List[Tuple[str, str, str, str]]) -> int:
        # content_rows: List of tuples (source_type, dataset_id, table_id, object_ref, content)
        # Build a VALUES clause via SELECT ... UNION ALL pattern
        if not content_rows:
            return 0

        selects: List[str] = []
        for idx, (source_type, dataset_id, table_id, object_ref) in enumerate(
            [(r[0], r[1], r[2], r[3]) for r in content_rows]
        ):
            content = content_rows[idx][4]
            # Escape single quotes
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
        job = self.client.query(sql)
        job.result()
        return job.num_dml_affected_rows or 0

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
        results = self.client.query(sql, job_config=job_config).result()
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
        results = self.client.query(sql, job_config=job_config).result()
        return [
            {"object_ref": r["object_ref"], "content": r["content"], "dist": float(r["dist"]) if r["dist"] is not None else None}
            for r in results
        ]