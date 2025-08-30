import React, { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { sankey as d3Sankey, sankeyLinkHorizontal, sankeyJustify } from 'd3-sankey'

type GraphNode = { id: string; type: 'database' | 'schema' | 'table' | 'column' | 'output' | 'join' | 'cte' | 'aggregation' | 'dataset' | 'metric' | 'kpi'; label?: string }
type GraphEdge = { source: string; target: string; type: 'join' | 'join_table' | 'projection' | 'contains' | 'derives' | 'dimension' | 'measure' | 'join_input' | 'join_output' }

type JoinInfo = { id?: string; left_table?: string; right_table?: string; type?: string; on?: string; pairs?: { left: string; right: string }[] }

type Outputs = { x?: string; y?: string; label?: string; value?: string }

export function LineageGraph({ graph, joins, outputs }: { graph: { nodes: GraphNode[]; edges: GraphEdge[] }; joins?: JoinInfo[]; outputs?: Outputs }) {
  const wrapRef = useRef<HTMLDivElement | null>(null)
  const svgRef = useRef<SVGSVGElement | null>(null)
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 800, h: 480 })

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setSize({ w: el.clientWidth, h: el.clientHeight }))
    ro.observe(el)
    setSize({ w: el.clientWidth, h: el.clientHeight })
    return () => ro.disconnect()
  }, [wrapRef.current])

  useEffect(() => {
    if (!svgRef.current) return
    const width = Math.max(200, size.w)
    const height = Math.max(200, size.h)
    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const g = svg.append('g')

    const zoom = d3.zoom<SVGSVGElement, unknown>().scaleExtent([0.3, 4]).on('zoom', (event) => {
      g.attr('transform', String(event.transform))
    })
    svg.call(zoom as any)

    // Consistent color mapping by node type
    const NODE_COLORS: Record<string, string> = {
      dataset: '#3B82F6',
      database: '#1D4ED8',
      schema: '#60A5FA',
      table: '#239BA7',
      column: '#64748B',
      cte: '#06B6D4',
      aggregation: '#EC4899',
      join: '#F59E0B',
      output: '#8B5CF6',
      metric: '#8B5CF6',
      kpi: '#8B5CF6',
      // KPI-first types
      Column: '#64748B',
      Dim: '#3B82F6',
      Grain: '#8B5CF6',
      Filter: '#10B981',
      Join: '#F59E0B',
      Calc: '#EC4899',
      Policy: '#F97316',
      KPI: '#8B5CF6',
    }
    const color = (d: GraphNode) => NODE_COLORS[d.type] || '#64748B'

    // 1 inch spacing ~ 96 px
    const NODE_PADDING = 96
    const NODE_WIDTH = 16
    const MARGIN = { top: 8, right: 8, bottom: 8, left: 8 }

    // Derive friendly output labels from SQL expressions
    function toTitleCase(s: string): string {
      return s.split(/[_\s]+/g).map(w => w ? (w[0].toUpperCase() + w.slice(1)) : w).join(' ')
    }
    function lastIdentifier(expr: string): string {
      const noQuotes = expr.replace(/["`]/g, '')
      const parts = noQuotes.split('.')
      return parts[parts.length - 1] || noQuotes
    }
    function friendlyFromColumn(expr: string): string {
      return toTitleCase(lastIdentifier(expr))
    }
    function friendlyFromFunc(expr: string): string {
      const m = expr.match(/^\s*(\w+)\s*\((.*)\)\s*$/i)
      if (!m) return toTitleCase(expr.replace(/["`]/g, ''))
      const fn = (m[1] || '').toLowerCase()
      const arg = m[2] || ''
      const fnMap: Record<string, string> = { avg: 'Average', sum: 'Total', count: 'Count', max: 'Max', min: 'Min' }
      const base = fnMap[fn] || fn.toUpperCase()
      const argCol = lastIdentifier(arg.replace(/\bdistinct\s+/i, '').trim())
      const synonyms: Record<string, string> = { 'sale_price': 'Order Value', 'sales': 'Revenue' }
      const prettyArg = synonyms[argCol] || toTitleCase(argCol)
      if (fn === 'count' && /\*/.test(arg)) return 'Count'
      return `${base} ${prettyArg}`
    }
    function stripAlias(sqlExpr?: string): string | undefined {
      if (!sqlExpr) return undefined
      return sqlExpr.replace(/\s+AS\s+\w+\s*$/i, '').trim()
    }
    const friendlyOutputs: Record<string, string> = {}
    if (outputs) {
      const entries: [keyof Outputs, string | undefined][] = [['label', outputs.label], ['value', outputs.value], ['x', outputs.x], ['y', outputs.y]]
      for (const [key, val] of entries) {
        const e = stripAlias(val)
        if (!e) continue
        if (/^\s*(avg|sum|count|min|max)\s*\(/i.test(e)) friendlyOutputs[String(key)] = friendlyFromFunc(e)
        else friendlyOutputs[String(key)] = friendlyFromColumn(e)
      }
    }

    // Build nodes and links tailored for Sankey
    const baseNodes: Record<string, GraphNode> = {}
    for (const n of graph.nodes || []) {
      baseNodes[n.id] = { ...n }
      if (baseNodes[n.id].type === 'output' && friendlyOutputs[n.id]) {
        baseNodes[n.id].label = friendlyOutputs[n.id]
      } else if (!baseNodes[n.id].label) {
        baseNodes[n.id].label = n.type === 'table' ? (n.label || n.id.split('.').slice(-1)[0]) : (n.label || n.id)
      }
    }

    // Compute output dependencies by table to help route join -> outputs
    const outputsSet = new Set<string>((graph.nodes || []).filter(n => (n.type === 'output' || n.type === 'metric')).map(n => n.id))
    const derivesEdges = (graph.edges || []).filter(e => e.type === 'derives')
    const outputsByTable: Record<string, Set<string>> = {}
    for (const e of derivesEdges) {
      if (!outputsSet.has(e.target)) continue
      outputsByTable[e.source] = outputsByTable[e.source] || new Set<string>()
      outputsByTable[e.source].add(e.target)
    }

    // Create join nodes and corresponding links (skip if enterprise join edges exist)
    const syntheticJoinLinks: { source: string; target: string; type: 'join_in' | 'join_out'; meta?: any }[] = []
    const hasEnterpriseJoinEdges = Array.isArray(graph?.edges) && graph.edges.some((e: any) => e?.type === 'join_input' || e?.type === 'join_output')
    if (!hasEnterpriseJoinEdges) {
      const effectiveJoins = Array.isArray(joins) ? joins : []
      for (let idx = 0; idx < effectiveJoins.length; idx++) {
        const j = effectiveJoins[idx]
        const joinId = `__join__${idx + 1}`
        const joinLabel = (j && j.id) ? String(j.id) : `JOIN ${idx + 1}`
        baseNodes[joinId] = { id: joinId, type: 'join', label: joinLabel }

        const lt = j.left_table || ''
        const rt = j.right_table || ''
        if (lt) syntheticJoinLinks.push({ source: lt, target: joinId, type: 'join_in', meta: { on: j.on, side: 'left', id: j.id, pairs: j.pairs || [] } })
        if (rt) syntheticJoinLinks.push({ source: rt, target: joinId, type: 'join_in', meta: { on: j.on, side: 'right', id: j.id, pairs: j.pairs || [] } })

        const contributingOutputs = new Set<string>()
        if (lt && outputsByTable[lt]) outputsByTable[lt].forEach(o => contributingOutputs.add(o))
        if (rt && outputsByTable[rt]) outputsByTable[rt].forEach(o => contributingOutputs.add(o))
        contributingOutputs.forEach(o => syntheticJoinLinks.push({ source: joinId, target: o, type: 'join_out', meta: { on: j.on, id: j.id } }))
      }
    }

    // Build remaining links from graph
    const allowedEdgeTypes = new Set([
      'contains', 'projection', 'derives', 'dimension', 'measure', 'join_input', 'join_output',
      // Thought Graph KPI-first edges
      'DEPENDS_ON', 'USES_COLUMN', 'JOINS_COLUMN', 'TESTED_BY', 'MITIGATES', 'REFINES', 'FILTERS_COLUMN'
    ])
    const rawLinks: { source: string; target: string; type: string; meta?: any }[] = []
    for (const e of graph.edges || []) {
      const et = (e as any).type
      if (!allowedEdgeTypes.has(et)) continue
      const src = (e as any).source ?? (e as any).from
      const tgt = (e as any).target ?? (e as any).to
      if (!src || !tgt) continue
      rawLinks.push({ source: src, target: tgt, type: et })
    }

    rawLinks.push(...syntheticJoinLinks)

    const usedIds = new Set<string>()
    for (const l of rawLinks) { usedIds.add(l.source); usedIds.add(l.target) }
    const nodes = Object.values(baseNodes).filter(n => usedIds.has(n.id))

    const validId = new Set(nodes.map(n => n.id))
    const links = rawLinks
      .filter(l => validId.has(l.source) && validId.has(l.target))
      .map(l => ({ source: l.source, target: l.target, value: 1, _type: l.type, _meta: l.meta }))

    // Safety guards: limit graph size and wrap in try/catch to avoid runtime errors
    const MAX_NODES = 800
    const MAX_LINKS = 2000
    const safeNodes = nodes.slice(0, MAX_NODES)
    const validId2 = new Set(safeNodes.map(n => n.id))
    const safeLinks = links.filter(l => validId2.has(l.source) && validId2.has(l.target)).slice(0, MAX_LINKS)

    const sankeyLayout = d3Sankey<any, any>()
      .nodeId((d: any) => d.id)
      .nodeWidth(Math.max(2, Math.min(48, NODE_WIDTH)))
      .nodePadding(Math.max(4, Math.min(160, NODE_PADDING)))
      .nodeAlign(sankeyJustify)
      .extent([[MARGIN.left, MARGIN.top], [Math.max(MARGIN.left + 10, width - MARGIN.right), Math.max(MARGIN.top + 10, height - MARGIN.bottom)]])

    let sankeyData: { nodes: (GraphNode & { x0: number; x1: number; y0: number; y1: number })[]; links: any[] }
    try {
      sankeyData = sankeyLayout({
        nodes: safeNodes.map(d => ({ ...d })),
        links: safeLinks.map(l => ({ ...l }))
      }) as unknown as { nodes: (GraphNode & { x0: number; x1: number; y0: number; y1: number })[]; links: any[] }
    } catch (err) {
      // Fallback to empty graph on layout error
      sankeyData = { nodes: [], links: [] } as any
    }

    // Draw links thin
    const link = g.append('g')
      .attr('fill', 'none')
      .attr('stroke-opacity', 0.45)
      .selectAll('path')
      .data(sankeyData.links)
      .enter()
      .append('path')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke', (d: any) => {
        const t = d._type
        if (t === 'projection') return '#94a3b8'
        if (t === 'contains') return '#38bdf8'
        if (t === 'join_in' || t === 'join_out' || t === 'join_input' || t === 'join_output') return '#f59e0b'
        if (t === 'derives') return '#22c55e'
        if (t === 'dimension') return '#3b82f6'
        if (t === 'measure') return '#fb923c'
        return '#999'
      })
      .attr('stroke-width', 2.4)
      .attr('stroke-linecap', 'round')

    link.append('title').text((d: any) => `${d.source?.label || d.source?.id} → ${d.target?.label || d.target?.id}`)

    const shortCol = (s: string) => {
      if (!s) return ''
      const parts = String(s).replace(/["`]/g, '').split('.')
      return parts.slice(-2).join('.')
    }
    const linkLabel = (d: any): string => {
      const t = d._type
      if (t === 'join_in') {
        const pairs = Array.isArray(d._meta?.pairs) ? d._meta.pairs : []
        const side = d._meta?.side === 'right' ? 'right' : 'left'
        const cols = pairs.map((p: any) => side === 'left' ? shortCol(p.left) : shortCol(p.right)).filter(Boolean)
        if (cols.length) {
          const text = cols.slice(0, 3).join(', ')
          return cols.length > 3 ? `${text}…` : text
        }
        if (d._meta?.id) return String(d._meta.id)
        return 'Join'
      }
      if (t === 'join_out' || t === 'join_output') {
        return d._meta?.id ? String(d._meta.id) : 'Join'
      }
      if (t === 'join_input') {
        return 'Join'
      }
      if (t === 'contains') {
        return shortCol(d.target?.id || '') || (d.target?.label || d.target?.id || 'Contains')
      }
      if (t === 'projection') {
        return shortCol(d.source?.id || '') || (d.source?.label || d.source?.id || 'Projection')
      }
      if (t === 'derives') {
        return d.target?.label || d.target?.id || 'Derives'
      }
      if (t === 'dimension') {
        return d.source?.label || d.source?.id || 'Dimension'
      }
      if (t === 'measure') {
        return d.source?.label || d.source?.id || 'Measure'
      }
      return `${d.source?.label || d.source?.id || ''} → ${d.target?.label || d.target?.id || ''}`
    }

    g.append('g')
      .attr('class', 'link-labels')
      .selectAll('text')
      .data(sankeyData.links)
      .enter()
      .append('text')
      .attr('x', (d: any) => (d.source?.x1 + d.target?.x0) / 2)
      .attr('y', (d: any) => (d.y0 + d.y1) / 2)
      .attr('text-anchor', 'middle')
      .attr('dy', '0.35em')
      .attr('font-size', 10)
      .attr('fill', 'currentColor')
      .style('pointer-events', 'none')
      .style('paint-order', 'stroke')
      .style('stroke', 'white')
      .style('stroke-width', '3px')
      .text((d: any) => linkLabel(d))

    const node = g.append('g')
      .selectAll('g')
      .data(sankeyData.nodes)
      .enter()
      .append('g')

    node.append('rect')
      .attr('x', (d: any) => d.x0)
      .attr('y', (d: any) => d.y0)
      .attr('height', (d: any) => Math.max(2, d.y1 - d.y0))
      .attr('width', (d: any) => Math.max(4, d.x1 - d.x0))
      .attr('fill', (d: any) => color(d as GraphNode))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1)

    node.append('title').text((d: any) => d.id)

    node.append('text')
      .attr('x', (d: any) => d.x1 + 8)
      .attr('y', (d: any) => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('font-size', 11)
      .attr('fill', 'currentColor')
      .text((d: any) => {
        const base = d.label || (d.type === 'table' ? d.id.split('.').slice(-1)[0] : d.id)
        try {
          const section = (d.props && d.props.section) ? String(d.props.section) : ''
          if (section && (d.type === 'KPI' || d.type === 'kpi')) return `${base} [${section}]`
        } catch {}
        return base
      })
      .style('pointer-events', 'none')

    setTimeout(() => {
      const bounds = (g.node() as SVGGElement | null)?.getBBox()
      if (bounds && isFinite(bounds.width) && isFinite(bounds.height) && bounds.width > 0 && bounds.height > 0) {
        const wScale = Math.max(0.3, Math.min(1.2, 0.9 * width / bounds.width))
        const hScale = Math.max(0.3, Math.min(1.2, 0.9 * height / bounds.height))
        const scale = Math.min(wScale, hScale)
        const tx = width / 2 - scale * (bounds.x + bounds.width / 2)
        const ty = height / 2 - scale * (bounds.y + bounds.height / 2)
        svg.transition().duration(350).call(zoom.transform as any, d3.zoomIdentity.translate(tx, ty).scale(scale))
      }
    }, 0)
  }, [graph, joins, outputs, size.w, size.h])

  return (
    <div ref={wrapRef} style={{ width: '100%', height: '100%' }}>
      <svg ref={svgRef} width={size.w} height={size.h} />
    </div>
  )
}

