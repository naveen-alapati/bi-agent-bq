from typing import Any, Dict, List, Optional, Tuple, Set

import sqlglot
from sqlglot import exp
try:
    # Prefer module import to handle version differences gracefully
    from sqlglot.optimizer import qualify as _qualify_mod  # type: ignore
except Exception:  # pragma: no cover - defensive for older sqlglot
    _qualify_mod = None  # type: ignore
from datetime import datetime, timezone
import re


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


def _collect_cte_map(expr: exp.Expression) -> Dict[str, exp.Expression]:
    """Return mapping of CTE name -> CTE expression body."""
    cte_map: Dict[str, exp.Expression] = {}
    for cte in expr.find_all(exp.CTE):
        try:
            alias_expr = cte.args.get("alias")
            name = alias_expr and getattr(alias_expr, "this", None) and alias_expr.this.name
            body = cte.this if isinstance(cte.this, exp.Expression) else None
            if name and body is not None:
                cte_map[str(name)] = body
        except Exception:
            continue
    return cte_map


def _collect_tables_excluding(expr: exp.Expression, exclude: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Collect table identifiers referenced in expr, splitting into (base_tables, referenced_ctes)
    based on provided exclude set of CTE names.
    """
    base_tables: Set[str] = set()
    referenced_ctes: Set[str] = set()
    for t in expr.find_all(exp.Table):
        name = _fq_table(t)
        if not name:
            continue
        short = name.split(".")[-1]
        if name in exclude or short in exclude:
            referenced_ctes.add(short if short in exclude else name)
        else:
            base_tables.add(name)
    return base_tables, referenced_ctes


def _expand_cte_base_dependencies(cte_map: Dict[str, exp.Expression]) -> Dict[str, Set[str]]:
    """
    For each CTE, compute the set of underlying base tables (excluding other CTEs),
    expanding through CTE-to-CTE references.
    """
    cte_names: Set[str] = set(cte_map.keys())
    base_deps: Dict[str, Set[str]] = {name: set() for name in cte_names}
    cte_refs: Dict[str, Set[str]] = {name: set() for name in cte_names}
    # Initial pass: collect direct base tables and CTE references per CTE
    for name, body in cte_map.items():
        bases, refs = _collect_tables_excluding(body, cte_names)
        base_deps[name].update(bases)
        # Normalize references to raw CTE names (last segment)
        for r in refs:
            cte_refs[name].add(r.split(".")[-1])
    # Expand references transitively until fixed point
    changed = True
    while changed:
        changed = False
        for name in cte_names:
            for ref in list(cte_refs.get(name, set())):
                if ref in base_deps:
                    before = len(base_deps[name])
                    base_deps[name].update(base_deps[ref])
                    if len(base_deps[name]) != before:
                        changed = True
    return base_deps


def _build_alias_map(expr: exp.Expression, sources: List[str]) -> Dict[str, str]:
    """Map table aliases and short names to fully-qualified table ids.
    Includes CTE/table short names so alias resolution works for columns like cte.col or alias.col.
    """
    alias_to_table: Dict[str, str] = {}
    # Table aliases
    for t in expr.find_all(exp.Table):
        try:
            alias_expr = t.args.get("alias")
            if alias_expr and getattr(alias_expr, "this", None):
                alias_name = alias_expr.this.name
                fq = _fq_table(t)
                if alias_name and fq:
                    alias_to_table[alias_name] = fq
        except Exception:
            continue
    # Short names for all discovered sources (table id last segment)
    for s in sources:
        short = s.split(".")[-1]
        if short and short not in alias_to_table:
            alias_to_table[short] = s
    return alias_to_table


def _clean_identifier(s: str) -> str:
    return s.replace('"', '').replace('`', '')


def _infer_table_from_column(col_id: str, alias_to_table: Dict[str, str]) -> Optional[str]:
    """Best-effort mapping from a column id string to a table id using alias map.
    Accepts forms like alias.col, db.table.col, project.db.table.col
    """
    raw = _clean_identifier(col_id)
    parts = raw.split(".")
    if len(parts) >= 4:
        # project.dataset.table.column
        return ".".join(parts[:3])
    if len(parts) == 3:
        # db.table.column
        return ".".join(parts[:2])
    if len(parts) == 2:
        qual = parts[0]
        if qual in alias_to_table:
            return alias_to_table[qual]
        # Could be table.column without alias
        return alias_to_table.get(qual)
    return None


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


def compute_lineage(sql: str, dialect: str = "bigquery", bq: Any = None) -> Dict[str, Any]:
    # Parse and qualify for reliable column resolution
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception as e:
        raise ValueError(f"SQL parse error: {e}")

    qualified = parsed
    # Attempt to qualify using whichever API is available in this sqlglot version
    if _qualify_mod is not None:
        try:
            if hasattr(_qualify_mod, "qualify_columns"):
                qualified = _qualify_mod.qualify_columns(parsed, dialect=dialect)  # type: ignore[attr-defined]
            elif hasattr(_qualify_mod, "qualify"):
                # Older API
                qualified = _qualify_mod.qualify(parsed, dialect=dialect)  # type: ignore[attr-defined]
        except Exception:
            qualified = parsed

    # CTE analysis: map each CTE to its underlying base tables
    cte_map = _collect_cte_map(qualified)
    cte_names: Set[str] = set(cte_map.keys())
    cte_base_deps = _expand_cte_base_dependencies(cte_map) if cte_map else {}

    # Sources: only base tables, excluding CTE names
    base_sources, _ = _collect_tables_excluding(qualified, cte_names)
    sources = sorted(base_sources)

    # Alias map (start with base tables only)
    alias_map = _build_alias_map(qualified, sources)
    # If a CTE ultimately maps to a single base table, allow aliasing the CTE name to that base
    for cte_name, bases in cte_base_deps.items():
        if len(bases) == 1:
            try:
                alias_map.setdefault(cte_name, next(iter(bases)))
            except Exception:
                pass

    joins = _join_details(qualified)
    # Rewrite join tables to base tables when CTE is uniquely resolved
    if joins:
        for j in joins:
            lt = str(j.get("left_table") or "")
            rt = str(j.get("right_table") or "")
            lt_short = lt.split(".")[-1]
            rt_short = rt.split(".")[-1]
            if lt_short in cte_base_deps and len(cte_base_deps[lt_short]) == 1:
                j["left_table"] = next(iter(cte_base_deps[lt_short]))
            if rt_short in cte_base_deps and len(cte_base_deps[rt_short]) == 1:
                j["right_table"] = next(iter(cte_base_deps[rt_short]))

    filters = _collect_filters(qualified)
    group_by = _collect_group_by(qualified)
    outputs = _collect_outputs(qualified)

    # Build graph nodes/edges
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    # Table nodes (base sources only)
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

    # Helper: for a column id, resolve contributing base tables considering CTEs
    def _resolve_base_tables_for_column(col_id: str) -> List[str]:
        table_id = _infer_table_from_column(col_id, alias_map)
        if table_id:
            # If this resolves directly to a CTE with multiple bases, expand
            short = table_id.split(".")[-1]
            if short in cte_base_deps and len(cte_base_deps[short]) >= 1:
                return sorted(list(cte_base_deps[short]))
            return [table_id]
        # Otherwise, try to resolve via the qualifier part (alias or cte name)
        raw = _clean_identifier(col_id)
        parts = raw.split(".")
        if len(parts) >= 2:
            qual = parts[0]
            if qual in cte_base_deps and len(cte_base_deps[qual]) >= 1:
                return sorted(list(cte_base_deps[qual]))
        return []

    # Connect columns to their owning base tables (contains edges)
    for n in col_nodes:
        nid = n.get("id") or ""
        for base in _resolve_base_tables_for_column(str(nid)):
            edges.append({"source": base, "target": nid, "type": "contains"})

    # Connect base tables to outputs when an output depends on any column from that table (derives edges)
    proj_edges = [e for e in col_edges if e.get("type") == "projection"]
    for e in proj_edges:
        col_id = str(e.get("source"))
        out_id = str(e.get("target"))
        bases = _resolve_base_tables_for_column(col_id)
        for base in bases:
            edges.append({"source": base, "target": out_id, "type": "derives"})

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

    # ================= Enterprise enrichments =================
    # Build metadata for physical hierarchy using BigQuery when available
    def _split_fqn(table_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        parts = [p for p in str(table_id).split('.') if p]
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            # Use default project if provided by bq
            proj = getattr(bq, 'project_id', None)
            return proj, parts[0], parts[1]
        if len(parts) == 1:
            return getattr(bq, 'project_id', None), None, parts[0]
        return None, None, None

    # Index nodes by id for augmentation and deduplication
    node_by_id: Dict[str, Dict[str, Any]] = {n['id']: n for n in nodes}

    # Physical nodes: databases (projects) and schemas (datasets)
    databases: Set[str] = set()
    schemas: Set[str] = set()
    table_meta_cache: Dict[str, Dict[str, Any]] = {}

    for t in sources:
        proj, ds, tb = _split_fqn(t)
        if proj:
            databases.add(proj)
        if proj and ds:
            schemas.add(f"{proj}.{ds}")
        # Enrich table node with metadata if possible
        meta: Dict[str, Any] = {"database": proj, "schema": ds, "rowCount": None, "owner": None}
        if bq and proj and ds and tb:
            try:
                # Prefer INFORMATION_SCHEMA for row count and column types
                rc = None
                try:
                    rc = bq.get_table_row_count_info_schema(proj, ds, tb)
                except Exception:
                    rc = None
                meta["rowCount"] = rc
                # Columns schema
                cols: Dict[str, Dict[str, Any]] = {}
                try:
                    cols = bq.get_columns_info_schema(proj, ds, tb) or {}
                except Exception:
                    cols = {}
                # Fallback to Table object for descriptions if available
                try:
                    table_obj = bq.client.get_table(f"{proj}.{ds}.{tb}")
                    for f in list(getattr(table_obj, 'schema', []) or []):
                        entry = cols.setdefault(f.name, {"dataType": getattr(f, 'field_type', None)})
                        if getattr(f, 'description', None) is not None:
                            entry["description"] = getattr(f, 'description')
                except Exception:
                    pass
                meta["columns"] = cols
            except Exception:
                pass
        table_meta_cache[t] = meta
        # apply to table node
        if t in node_by_id:
            node_by_id[t].update({k: v for k, v in meta.items() if k != 'columns'})

    # Create database and schema nodes and containment edges
    for db in sorted(databases):
        if db not in node_by_id:
            node_by_id[db] = {"id": db, "type": "database", "label": db}
    for sch in sorted(schemas):
        if sch not in node_by_id:
            node_by_id[sch] = {"id": sch, "type": "schema", "label": sch.split('.')[-1]}
        # contains edge database -> schema
        db = sch.split('.')[0]
        edges.append({"source": db, "target": sch, "type": "contains"})
    for t in sources:
        proj, ds, _tb = _split_fqn(t)
        if proj and ds:
            edges.append({"source": f"{proj}.{ds}", "target": t, "type": "contains"})

    # Enrich column nodes with metadata (dataType, pii, description)
    for n in list(node_by_id.values()):
        if n.get('type') == 'column':
            nid = str(n['id'])
            base_tables = _resolve_base_tables_for_column(nid)
            col_name = _clean_identifier(nid).split('.')[-1]
            found = False
            for bt in base_tables:
                meta = table_meta_cache.get(bt) or {}
                cols = (meta.get('columns') or {})
                if col_name in cols:
                    n['dataType'] = cols[col_name].get('dataType')
                    n['description'] = cols[col_name].get('description')
                    found = True
                    break
            # PII/PHI placeholder
            n['pii'] = False

    # Logical: join nodes and edges (left/right input to join node)
    join_node_ids: List[str] = []
    for idx, j in enumerate(joins):
        jid = j.get('id') or f"join_{idx + 1}"
        jtype = (j.get('type') or '').upper() or 'JOIN'
        node_by_id[jid] = {
            "id": jid,
            "type": "join",
            "label": j.get('on') or jtype,
            "joinType": jtype,
            "condition": j.get('on') or '',
        }
        join_node_ids.append(jid)
        # connect pairs to join node as inputs
        for p in (j.get('pairs') or []):
            l = p.get('left'); r = p.get('right')
            if l:
                edges.append({"source": l, "target": jid, "type": "join_input"})
            if r:
                edges.append({"source": r, "target": jid, "type": "join_input"})

    # Aggregation nodes: derive from outputs that look like aggregate functions
    agg_nodes: List[str] = []
    def _is_agg(expr_sql: str) -> Optional[Tuple[str, str]]:
        m = re.match(r"\s*(\w+)\s*\((.*)\)\s*$", expr_sql, re.IGNORECASE)
        if not m:
            return None
        fn = m.group(1).upper()
        arg = m.group(2)
        if fn in {"AVG", "SUM", "COUNT", "MIN", "MAX"}:
            return fn, arg
        return None

    for key in ["value", "y"]:
        expr_sql = outputs.get(key) if isinstance(outputs, dict) else None
        if not expr_sql:
            continue
        parsed = _is_agg(expr_sql)
        if not parsed:
            continue
        fn, arg = parsed
        aid = f"aggregation_{len(agg_nodes) + 1}"
        node_by_id[aid] = {"id": aid, "type": "aggregation", "label": f"{fn}({arg})"}
        agg_nodes.append(aid)
        # measure edge from column(s) inside arg to aggregation
        # pick first column reference inside arg
        try:
            arg_expr = sqlglot.parse_one(f"SELECT {arg}")
            first_col = next((c.sql() for c in arg_expr.find_all(exp.Column)), None)
        except Exception:
            first_col = None
        if first_col:
            edges.append({"source": first_col, "target": aid, "type": "measure"})
        # Join output to aggregation (if join exists)
        for jid in join_node_ids:
            edges.append({"source": jid, "target": aid, "type": "join_output"})

    # Semantic: KPI node capturing definition and ownership
    kpi_id = "kpi_1"
    kpi_node = {
        "id": kpi_id,
        "type": "kpi",
        "label": "KPI",
        "definition": (outputs.get("value") or outputs.get("y") or "").strip() if isinstance(outputs, dict) else "",
        "owner": "Naveen Alapati",
        "lastModified": datetime.now(timezone.utc).isoformat(),
        "downstream": [],
    }
    node_by_id[kpi_id] = kpi_node

    # Ensure group-by expressions exist as nodes
    for gexpr in group_by:
        if gexpr not in node_by_id:
            node_by_id[gexpr] = {"id": gexpr, "type": "column", "label": gexpr}
    # Dimension edges from group-by expressions to KPI
    for gexpr in group_by:
        edges.append({"source": gexpr, "target": kpi_id, "type": "dimension"})

    # Derivation edges from aggregations to KPI
    for aid in agg_nodes:
        edges.append({"source": aid, "target": kpi_id, "type": "derives"})

    # Rebuild nodes list from map
    nodes = list(node_by_id.values())

    # Governance metadata
    governance = {
        "createdBy": "cursor_ai",
        "lastModified": datetime.now(timezone.utc).isoformat(),
        "lineageVersion": "1.0",
    }

    # Hierarchy summary
    hierarchy: Dict[str, Any] = {
        "physical": {
            "databases": sorted(list(databases)),
            "schemas": sorted(list(schemas)),
            "tables": sorted(list(sources)),
            "columns": sorted([n['id'] for n in nodes if n.get('type') == 'column']),
        },
        "logical": {
            "joins": join_node_ids,
            "filters": filters,
            "aggregations": agg_nodes,
        },
        "semantic": {
            "kpis": [kpi_id],
            "dashboards": [],
        },
    }

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
        "nodes": nodes,  # enterprise style top-level
        "edges": edges,  # enterprise style top-level
        "governance": governance,
        "hierarchy": hierarchy,
    }
    return result

