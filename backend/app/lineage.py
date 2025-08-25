from typing import Any, Dict, List, Optional, Tuple, Set

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify_columns


def _fq_table(t: exp.Expression) -> str:
    if isinstance(t, exp.Table):
        # Try to build project.dataset.table if available, else just table/alias
        catalog = t.args.get("catalog") and t.args["catalog"].name
        db = t.args.get("db") and t.args["db"].name
        table = t.this and t.this.name
        parts = [p for p in [catalog, db, table] if p]
        return ".".join(parts) if parts else (table or "")
    return t.sql()


def _collect_tables(expr: exp.Expression) -> List[str]:
    tables: Set[str] = set()
    for t in expr.find_all(exp.Table):
        tables.add(_fq_table(t))
    return sorted([t for t in tables if t])


def _join_details(expr: exp.Expression) -> List[Dict[str, Any]]:
    joins: List[Dict[str, Any]] = []
    for j in expr.find_all(exp.Join):
        on_sql = j.args.get("on").sql() if j.args.get("on") is not None else (j.args.get("using") and j.args.get("using").sql()) or ""
        kind = (j.args.get("kind") or "").upper() if isinstance(j.args.get("kind"), str) else (j.args.get("kind") or "")
        right = j.this
        right_alias = (right.args.get("alias") and right.args["alias"].this and right.args["alias"].this.name) if isinstance(right, exp.Expression) else None
        right_table = _fq_table(right)

        # Infer left side by climbing to parent FROM or previous joins
        parent = j.parent
        left_table = ""
        if isinstance(parent, exp.From):
            base = parent.this
            left_table = _fq_table(base)
        else:
            # Fallback: previous table in the FROM joins chain
            for prev in j.parent.find_all(exp.Table):
                left_table = _fq_table(prev)
                break

        pairs: List[Tuple[str, str]] = []
        # Extract column pairs from ON conditions like a.col = b.col
        if j.args.get("on") is not None:
            for comp in j.args["on"].find_all(exp.EQ):
                l = comp.left.sql()
                r = comp.right.sql()
                pairs.append((l, r))
        # USING(col, ...)
        if j.args.get("using") is not None:
            cols = [c.name for c in j.args["using"].find_all(exp.Identifier)]
            for c in cols:
                # Build best-effort qualified cols
                l = f"{left_table.split('.')[-1]}.{c}" if left_table else c
                r = f"{right_alias or right_table.split('.')[-1]}.{c}" if (right_alias or right_table) else c
                pairs.append((l, r))

        joins.append({
            "left_table": left_table,
            "right_table": right_table,
            "right_alias": right_alias,
            "type": str(kind or "").upper(),
            "on": on_sql,
            "pairs": [{"left": a, "right": b} for a, b in pairs],
        })
    return joins


def _collect_filters(expr: exp.Expression) -> List[str]:
    out: List[str] = []
    for where in expr.find_all(exp.Where):
        if where.this is not None:
            out.append(where.this.sql())
    # HAVING filters
    for having in expr.find_all(exp.Having):
        if having.this is not None:
            out.append(having.this.sql())
    return out


def _collect_group_by(expr: exp.Expression) -> List[str]:
    out: List[str] = []
    for gb in expr.find_all(exp.Group):
        for e in gb.expressions:
            out.append(e.sql())
    return out


def _collect_outputs(expr: exp.Expression) -> Dict[str, Any]:
    outputs: Dict[str, Any] = {}
    select = expr.find(exp.Select)
    if not select:
        return outputs
    for proj in select.expressions:
        alias = proj.alias_or_name
        if alias:
            label = alias
            if label.lower() in ("x", "y", "label", "value"):
                outputs[label.lower()] = proj.sql()
    return outputs


def _collect_column_lineage(expr: exp.Expression) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (column_nodes, column_edges) capturing:
      - nodes: { id: table.col or alias, type: 'column', table?: fq_table }
      - edges: projection (source col -> output alias), and equality edges from joins
    """
    column_nodes: Dict[str, Dict[str, Any]] = {}
    column_edges: List[Dict[str, Any]] = []

    # Build mapping from alias outputs to their source columns using expression dependencies
    select = expr.find(exp.Select)
    if select:
        for proj in select.expressions:
            alias = proj.alias_or_name
            if alias:
                # Find all column references under this projection
                refs = [c for c in proj.find_all(exp.Column)]
                for c in refs:
                    col_id = c.sql()  # may already be qualified after qualify_columns
                    column_nodes.setdefault(col_id, {"id": col_id, "type": "column"})
                    out_id = alias
                    column_nodes.setdefault(out_id, {"id": out_id, "type": "output"})
                    column_edges.append({"source": col_id, "target": out_id, "type": "projection"})

    # Join equality edges
    for j in expr.find_all(exp.Join):
        if j.args.get("on") is not None:
            for comp in j.args["on"].find_all(exp.EQ):
                l = comp.left.sql()
                r = comp.right.sql()
                column_nodes.setdefault(l, {"id": l, "type": "column"})
                column_nodes.setdefault(r, {"id": r, "type": "column"})
                column_edges.append({"source": l, "target": r, "type": "join"})
        if j.args.get("using") is not None:
            for c in j.args["using"].find_all(exp.Identifier):
                name = c.name
                # We cannot be sure of left/right aliases post-qualification; just add a neutral using edge
                column_nodes.setdefault(name, {"id": name, "type": "column"})
    return list(column_nodes.values()), column_edges


def compute_lineage(sql: str, dialect: str = "bigquery") -> Dict[str, Any]:
    # Parse and qualify for reliable column resolution
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception as e:
        raise ValueError(f"SQL parse error: {e}")

    try:
        qualified = qualify_columns(parsed, dialect=dialect)
    except Exception:
        qualified = parsed

    sources = _collect_tables(qualified)
    joins = _join_details(qualified)
    filters = _collect_filters(qualified)
    group_by = _collect_group_by(qualified)
    outputs = _collect_outputs(qualified)

    # Build graph nodes/edges
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    # Table nodes
    for t in sources:
        nodes.append({"id": t, "type": "table", "label": t.split(".")[-1]})

    # Column nodes and edges
    col_nodes, col_edges = _collect_column_lineage(qualified)
    # Attach table info to column nodes when possible by heuristic split of qualified name
    for n in col_nodes:
        nid = n.get("id") or ""
        if "." in nid:
            # could be alias.col or db.table.col; leave as-is
            pass
    nodes.extend(col_nodes)
    edges.extend(col_edges)

    # Add join table-level edges for visualization
    for j in joins:
        lt = j.get("left_table")
        rt = j.get("right_table")
        if lt and rt:
            edges.append({"source": lt, "target": rt, "type": "join_table", "on": j.get("on", "")})

    # Add filter/groupBy dependency placeholder edges (from referenced columns)
    # Filters
    for f in filters:
        # Extract column refs via re-parsing small fragments is heavy; keep textual record for now
        pass

    result: Dict[str, Any] = {
        "sources": sources,
        "joins": joins,
        "filters": filters,
        "groupBy": group_by,
        "outputs": outputs if outputs else None,
        "graph": {
            "nodes": nodes,
            "edges": edges,
        },
    }
    return result

