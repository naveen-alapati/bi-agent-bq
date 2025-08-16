from __future__ import annotations
from enum import Enum
from typing import List, Dict, Any, Optional, Tuple
import os
import time
import uuid
from datetime import datetime, timezone

from google.cloud import aiplatform
from google.api_core.client_options import ClientOptions

from .bq import BigQueryService


class EmbeddingMode(str, Enum):
    bigquery = "bigquery"
    vertex = "vertex"
    openai = "openai"


class EmbeddingService:
    def __init__(
        self,
        mode: EmbeddingMode,
        project_id: Optional[str],
        location: str,
        bq_dataset: str,
    ) -> None:
        self.mode = mode
        self.project_id = project_id
        self.location = location
        self.bq_dataset = bq_dataset
        self.vertex_model = os.getenv("VERTEX_EMBEDDING_MODEL", "textembedding-gecko@003")
        self.vertex_location = os.getenv("VERTEX_LOCATION", location)
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.bqml_model_fqn = os.getenv("BQ_EMBEDDING_MODEL_FQN", "")  # e.g. project.dataset.embedding_model

    @staticmethod
    def build_table_summary_content(
        project_id: str,
        dataset_id: str,
        table_id: str,
        schema: List[Dict[str, Any]],
        samples: List[Dict[str, Any]],
    ) -> str:
        cols = ", ".join([f"{c['name']}:{c['type']}" for c in schema])
        lines = [
            f"table: {dataset_id}.{table_id}",
            f"columns: {cols}",
            f"samples:",
        ]
        for row in samples:
            lines.append(str(row))
        return "\n".join(lines)

    def generate_and_store_embeddings(
        self,
        bq: BigQueryService,
        rows: List[Tuple[str, str, str, str, str]],
    ) -> int:
        # rows: (source_type, dataset_id, table_id, object_ref, content)
        target_table = bq.ensure_embeddings_table(self.bq_dataset, table_name="table_embeddings")
        if self.mode == EmbeddingMode.bigquery:
            if not self.bqml_model_fqn:
                raise RuntimeError("BQ_EMBEDDING_MODEL_FQN must be set for bigquery embedding mode.")
            return bq.run_embedding_insert_with_bqml(self.bqml_model_fqn, target_table, rows)

        if self.mode == EmbeddingMode.vertex:
            return self._generate_with_vertex_and_insert(bq, target_table, rows)

        if self.mode == EmbeddingMode.openai:
            return self._generate_with_openai_and_insert(bq, target_table, rows)

        raise RuntimeError("Unsupported embedding mode")

    def _generate_with_vertex_and_insert(
        self,
        bq: BigQueryService,
        table_fqn: str,
        rows: List[Tuple[str, str, str, str, str]],
    ) -> int:
        aiplatform.init(project=self.project_id, location=self.vertex_location)
        from google.cloud import aiplatform_v1

        client = aiplatform_v1.EmbeddingServiceClient(
            client_options=ClientOptions(api_endpoint=f"{self.vertex_location}-aiplatform.googleapis.com")
        )
        instances = []
        for _, _, _, _, content in rows:
            instances.append({"content": content})
        # Batch in small chunks to respect quotas
        embeddings: List[List[float]] = []
        batch_size = 16
        for i in range(0, len(instances), batch_size):
            batch = instances[i : i + batch_size]
            resp = client.batch_embed_text(
                model=self.vertex_model,
                requests=[
                    aiplatform_v1.EmbedTextRequest(
                        model=self.vertex_model,
                        content=item["content"],
                    )
                    for item in batch
                ],
            )
            for r in resp.embeddings:
                embeddings.append(list(r.values))
            time.sleep(0.2)
        now_iso = datetime.now(timezone.utc).isoformat()
        json_rows = []
        for idx, (source_type, dataset_id, table_id, object_ref, content) in enumerate(rows):
            json_rows.append(
                {
                    "id": uuid.uuid4().hex,
                    "source_type": source_type,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "object_ref": object_ref,
                    "content": content,
                    "embedding": embeddings[idx],
                    "created_at": now_iso,
                }
            )
        bq.insert_embeddings_json(table_fqn, json_rows)
        return len(json_rows)

    def _generate_with_openai_and_insert(
        self,
        bq: BigQueryService,
        table_fqn: str,
        rows: List[Tuple[str, str, str, str, str]],
    ) -> int:
        if not self.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY must be set for openai embedding mode.")
        try:
            from openai import OpenAI
        except Exception as exc:
            raise RuntimeError("openai package not installed. Add to requirements.txt") from exc
        client = OpenAI(api_key=self.openai_api_key)
        embeddings: List[List[float]] = []
        batch_size = 16
        contents = [r[4] for r in rows]
        for i in range(0, len(contents), batch_size):
            batch = contents[i : i + batch_size]
            resp = client.embeddings.create(input=batch, model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"))
            for item in resp.data:
                embeddings.append(item.embedding)
        now_iso = datetime.now(timezone.utc).isoformat()
        json_rows = []
        for idx, (source_type, dataset_id, table_id, object_ref, content) in enumerate(rows):
            json_rows.append(
                {
                    "id": uuid.uuid4().hex,
                    "source_type": source_type,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "object_ref": object_ref,
                    "content": content,
                    "embedding": embeddings[idx],
                    "created_at": now_iso,
                }
            )
        bq.insert_embeddings_json(table_fqn, json_rows)
        return len(json_rows)

    def embed_text(self, text: str) -> List[float]:
        if self.mode == EmbeddingMode.vertex:
            from google.cloud import aiplatform_v1
            client = aiplatform_v1.EmbeddingServiceClient(
                client_options=ClientOptions(api_endpoint=f"{self.vertex_location}-aiplatform.googleapis.com")
            )
            resp = client.embed_text(model=self.vertex_model, content=text)
            return list(resp.embedding.values)
        if self.mode == EmbeddingMode.openai:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)
            resp = client.embeddings.create(input=[text], model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"))
            return list(resp.data[0].embedding)
        raise RuntimeError("embed_text is only used for external modes. For bigquery mode, compute embeddings inside SQL.")