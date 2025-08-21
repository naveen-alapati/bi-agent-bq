import json
import os
import sys
from typing import Any, Dict


def _ensure_project_id() -> str:
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise RuntimeError("PROJECT_ID environment variable is required")
    return project_id


def _connect_services(project_id: str):
    # Allow importing backend app modules when running as a script
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, repo_root)
    from app.bq import BigQueryService  # type: ignore

    bq = BigQueryService(project_id=project_id)
    return bq


def create_or_replace_view(bq, view_sql: str) -> None:
    # Execute DDL in the dataset location or default location
    print("Creating or replacing view with provided SQL DDL...")
    bq.client.query(view_sql, location=bq.location).result()
    print("View created/replaced successfully.")


def seed_kpi(bq, item: Dict[str, Any], dashboards_dataset: str = "analytics_dash") -> None:
    print("Seeding KPI into catalog...")
    inserted = bq.add_to_kpi_catalog([item], dataset_id=dashboards_dataset)
    print(f"Inserted {inserted} KPI(s) into catalog.")


def main():
    project_id = _ensure_project_id()
    bq = _connect_services(project_id)

    # 1) Create or replace the view in BigQuery, as provided
    view_sql = (
        """
CREATE OR REPLACE VIEW `numeric-abbey-469615-m7.ecart.vw_daily_sales` AS
SELECT
  f.order_date,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.quantity)            AS units,
  SUM(f.net_revenue_ex_tax)  AS net_sales_ex_tax,
  SUM(f.gross_margin)        AS gross_margin,
  SAFE_DIVIDE(SUM(f.net_revenue_ex_tax), COUNT(DISTINCT f.order_id)) AS avg_order_value
FROM `numeric-abbey-469615-m7.ecart.fact_sales` f
GROUP BY f.order_date;
        """
    ).strip()

    create_or_replace_view(bq, view_sql)

    # 2) Seed KPI Catalog entry for "Daily sales summary (net ex-tax) and orders"
    # Use a vega-lite spec with fold transform to plot both metrics over time.
    vega_lite_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "mark": {"type": "line", "point": True},
        "transform": [
            {"fold": ["orders", "net_sales_ex_tax"], "as": ["metric", "value"]}
        ],
        "encoding": {
            "x": {"field": "order_date", "type": "temporal", "title": "Date"},
            "y": {"field": "value", "type": "quantitative", "title": "Value"},
            "color": {"field": "metric", "type": "nominal", "title": "Metric"},
            "tooltip": [
                {"field": "order_date", "type": "temporal", "title": "Date"},
                {"field": "metric", "type": "nominal"},
                {"field": "value", "type": "quantitative"}
            ]
        }
    }

    kpi_sql = (
        """
SELECT
  order_date,
  orders,
  net_sales_ex_tax
FROM `numeric-abbey-469615-m7.ecart.vw_daily_sales`
ORDER BY order_date
        """
    ).strip()

    item = {
        "name": "Daily sales summary (net ex-tax) and orders",
        "sql": kpi_sql,
        "chart_type": "line",
        "expected_schema": "timeseries",
        "dataset_id": "ecart",
        "table_id": "vw_daily_sales",
        "tags": {"source": "seed_script", "metrics": ["orders", "net_sales_ex_tax"]},
        "engine": "vega-lite",
        "vega_lite_spec": vega_lite_spec,
    }

    dashboards_dataset = os.getenv("DASHBOARDS_DATASET", "analytics_dash")
    seed_kpi(bq, item, dashboards_dataset)

    print(
        "Done. You can now open the KPI Catalog in the UI and add 'Daily sales summary (net ex-tax) and orders' to your dashboard."
    )


if __name__ == "__main__":
    main()

