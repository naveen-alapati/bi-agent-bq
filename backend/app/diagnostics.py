from typing import Any, Dict, List, Optional
import time
import traceback
import os

from .bq import BigQueryService
from .kpi import KPIService
from .models import TableRef


def run_self_test(
    bq: BigQueryService,
    kpi: KPIService,
    dataset: Optional[str] = None,
    limit_tables: int = 3,
    sample_rows: int = 3,
    kpis_k: int = 3,
    run_kpis_limit: int = 2,
    force_llm: bool = True,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {"steps": []}

    # LLM diagnostics
    try:
        llm_diag = kpi.llm.diagnostics()
        results["llm"] = llm_diag
        results["steps"].append({"step": "llm", "status": "ok" if llm_diag.get("ok") else "error", "provider": llm_diag.get("provider"), "detail": llm_diag})
    except Exception as exc:
        results["steps"].append({"step": "llm", "status": "error", "error": str(exc), "stack": traceback.format_exc()})

    # Step: datasets
    try:
        t0 = time.time()
        datasets = bq.list_datasets()
        dt = time.time() - t0
        results["datasets"] = datasets
        results["steps"].append({"step": "list_datasets", "status": "ok", "count": len(datasets), "ms": int(dt * 1000)})
    except Exception as exc:
        results["steps"].append({"step": "list_datasets", "status": "error", "error": str(exc), "stack": traceback.format_exc()})
        return results

    chosen_dataset = dataset
    if not chosen_dataset:
        if any(d.get("datasetId") == "ecom" for d in datasets):
            chosen_dataset = "ecom"
        elif datasets:
            chosen_dataset = datasets[0]["datasetId"]
        else:
            results["steps"].append({"step": "choose_dataset", "status": "error", "error": "No datasets available"})
            return results
    results["dataset"] = chosen_dataset

    # Step: tables
    try:
        t0 = time.time()
        tables_resp = bq.list_tables(chosen_dataset)
        dt = time.time() - t0
        results["tables"] = tables_resp
        results["steps"].append({"step": "list_tables", "status": "ok", "count": len(tables_resp), "ms": int(dt * 1000)})
    except Exception as exc:
        results["steps"].append({"step": "list_tables", "status": "error", "error": str(exc), "stack": traceback.format_exc()})
        return results

    selected_tables = [TableRef(projectId=bq.project_id, datasetId=chosen_dataset, tableId=t["tableId"]) for t in tables_resp[: max(1, min(limit_tables, len(tables_resp)))] ]
    results["selected_tables"] = [{"datasetId": t.datasetId, "tableId": t.tableId} for t in selected_tables]

    # Step: prepare
    try:
        t0 = time.time()
        prepared = kpi.prepare_tables(selected_tables, sample_rows=sample_rows)
        dt = time.time() - t0
        results["prepare"] = [{"datasetId": p.datasetId, "tableId": p.tableId, "embed_rows": p.embed_rows} for p in prepared]
        results["steps"].append({"step": "prepare", "status": "ok", "ms": int(dt * 1000)})
    except Exception as exc:
        results["steps"].append({"step": "prepare", "status": "error", "error": str(exc), "stack": traceback.format_exc()})

    # Step: generate_kpis (LLM-only if requested)
    if force_llm:
        os.environ["KPI_FALLBACK_ENABLED"] = "false"
    try:
        t0 = time.time()
        kpis = kpi.generate_kpis(selected_tables, k=kpis_k)
        dt = time.time() - t0
        results["kpis"] = [k.model_dump() for k in kpis]
        results["steps"].append({"step": "generate_kpis", "status": "ok", "count": len(kpis), "ms": int(dt * 1000)})
    except Exception as exc:
        results["steps"].append({"step": "generate_kpis", "status": "error", "error": str(exc), "stack": traceback.format_exc()})
        return results

    # Step: run_kpi (first few)
    run_summaries: List[Dict[str, Any]] = []
    for kpi_item in (results.get("kpis") or [])[: max(1, run_kpis_limit)]:
        sql = kpi_item.get("sql", "")
        if not sql:
            run_summaries.append({"id": kpi_item.get("id"), "status": "skip", "reason": "no sql"})
            continue
        try:
            t0 = time.time()
            rows = bq.query_rows(sql)
            dt = time.time() - t0
            run_summaries.append({
                "id": kpi_item.get("id"),
                "status": "ok",
                "rows": len(rows),
                "first_row": rows[0] if rows else None,
                "ms": int(dt * 1000),
            })
        except Exception as exc:
            run_summaries.append({
                "id": kpi_item.get("id"),
                "status": "error",
                "error": str(exc),
                "stack": traceback.format_exc(),
            })
    results["run_kpi"] = run_summaries

    return results