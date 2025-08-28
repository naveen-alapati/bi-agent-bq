-- Example vector search query template
-- Parameters: project_id, embeddings_dataset, query_embedding ARRAY<FLOAT64>
-- Replace placeholders at runtime if needed.
SELECT
  te.*,
  VECTOR_DISTANCE(te.embedding, @query_embedding) AS dist
FROM `PROJECT_ID.analytics_poc.table_embeddings` AS te
-- In modern BigQuery, prefer ordering by distance rather than VECTOR_SEARCH predicate
ORDER BY dist
LIMIT 25;