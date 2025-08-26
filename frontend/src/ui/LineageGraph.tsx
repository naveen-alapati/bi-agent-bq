import React, { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { sankey as d3Sankey, sankeyLinkHorizontal, sankeyJustify } from 'd3-sankey'

type GraphNode = { id: string; type: 'table' | 'column' | 'output' | 'join'; label?: string }
type GraphEdge = { source: string; target: string; type: 'join' | 'join_table' | 'projection' | 'contains' | 'derives' }

type JoinInfo = { left_table?: string; right_table?: string; type?: string; on?: string; pairs?: { left: string; right: string }[] }

export function LineageGraph({ graph, joins }: { graph: { nodes: GraphNode[]; edges: GraphEdge[] }; joins?: JoinInfo[] }) {
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

    const color = (d: GraphNode) => d.type === 'table' ? '#239BA7' : d.type === 'output' ? '#8B5CF6' : d.type === 'join' ? '#F59E0B' : '#64748B'

    // 1 inch spacing ~ 96 px
    const NODE_PADDING = 96
    const NODE_WIDTH = 16
    const MARGIN = { top: 8, right: 8, bottom: 8, left: 8 }

    // Build nodes and links tailored for Sankey
    const baseNodes: Record<string, GraphNode> = {}
    for (const n of graph.nodes || []) {
      baseNodes[n.id] = { ...n }
      if (!baseNodes[n.id].label) {
        baseNodes[n.id].label = n.type === 'table' ? (n.label || n.id.split('.').slice(-1)[0]) : (n.label || n.id)
      }
    }

    // Compute output dependencies by table to help route join -> outputs
    const outputs = new Set<string>((graph.nodes || []).filter(n => n.type === 'output').map(n => n.id))
    const derivesEdges = (graph.edges || []).filter(e => e.type === 'derives')
    const outputsByTable: Record<string, Set<string>> = {}
    for (const e of derivesEdges) {
      if (!outputs.has(e.target)) continue
      outputsByTable[e.source] = outputsByTable[e.source] || new Set<string>()
      outputsByTable[e.source].add(e.target)
    }

    // Create join nodes and corresponding links
    const joinNodeIds: string[] = []
    const syntheticJoinLinks: { source: string; target: string; type: 'join_in' | 'join_out'; meta?: any }[] = []
    const effectiveJoins = Array.isArray(joins) ? joins : []
    for (let idx = 0; idx < effectiveJoins.length; idx++) {
      const j = effectiveJoins[idx]
      const joinId = `__join__${idx + 1}`
      const joinLabel = `JOIN ${idx + 1}`
      baseNodes[joinId] = { id: joinId, type: 'join', label: joinLabel }
      joinNodeIds.push(joinId)

      const lt = j.left_table || ''
      const rt = j.right_table || ''
      if (lt) syntheticJoinLinks.push({ source: lt, target: joinId, type: 'join_in', meta: { on: j.on, side: 'left' } })
      if (rt) syntheticJoinLinks.push({ source: rt, target: joinId, type: 'join_in', meta: { on: j.on, side: 'right' } })

      // Route join -> outputs where either side contributes
      const contributingOutputs = new Set<string>()
      if (lt && outputsByTable[lt]) outputsByTable[lt].forEach(o => contributingOutputs.add(o))
      if (rt && outputsByTable[rt]) outputsByTable[rt].forEach(o => contributingOutputs.add(o))
      contributingOutputs.forEach(o => syntheticJoinLinks.push({ source: joinId, target: o, type: 'join_out', meta: { on: j.on } }))
    }

    // Build remaining links from graph
    const allowedEdgeTypes = new Set(['contains', 'projection', 'derives'])
    const rawLinks: { source: string; target: string; type: string; meta?: any }[] = []
    for (const e of graph.edges || []) {
      if (!allowedEdgeTypes.has(e.type)) continue
      rawLinks.push({ source: e.source, target: e.target, type: e.type })
    }

    // Merge synthetic join links
    rawLinks.push(...syntheticJoinLinks)

    // Filter out isolated nodes (keep only nodes that appear in any link)
    const usedIds = new Set<string>()
    for (const l of rawLinks) { usedIds.add(l.source); usedIds.add(l.target) }
    const nodes = Object.values(baseNodes).filter(n => usedIds.has(n.id))

    // Map node id -> index
    const idToIndex = new Map<string, number>(nodes.map((n, i) => [n.id, i]))

    // Convert links to index-based for sankey
    const links = rawLinks
      .filter(l => idToIndex.has(l.source) && idToIndex.has(l.target))
      .map(l => ({ source: idToIndex.get(l.source) as number, target: idToIndex.get(l.target) as number, value: 1, _type: l.type, _meta: l.meta }))

    // Prepare sankey layout
    const sankeyLayout = d3Sankey<any, any>()
      .nodeId((d: any) => d.id)
      .nodeWidth(NODE_WIDTH)
      .nodePadding(NODE_PADDING)
      .nodeAlign(sankeyJustify)
      .extent([[MARGIN.left, MARGIN.top], [width - MARGIN.right, height - MARGIN.bottom]])

    const sankeyData = sankeyLayout({
      nodes: nodes.map(d => ({ ...d })),
      links: links.map(l => ({ ...l }))
    }) as unknown as { nodes: (GraphNode & { x0: number; x1: number; y0: number; y1: number })[]; links: any[] }

    // Draw links
    const link = g.append('g')
      .attr('fill', 'none')
      .attr('stroke-opacity', 0.4)
      .selectAll('path')
      .data(sankeyData.links)
      .enter()
      .append('path')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke', (d: any) => {
        const t = d._type
        if (t === 'projection') return '#94a3b8'
        if (t === 'contains') return '#38bdf8'
        if (t === 'join_in' || t === 'join_out') return '#f59e0b'
        if (t === 'derives') return '#22c55e'
        return '#999'
      })
      .attr('stroke-width', (d: any) => Math.max(1, d.width))
      .attr('stroke-linecap', 'round')

    link.append('title').text((d: any) => `${nodes[d.source.index].label || nodes[d.source.index].id} → ${nodes[d.target.index].label || nodes[d.target.index].id}`)

    // Draw nodes
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

    // Labels: to the right of the node
    node.append('text')
      .attr('x', (d: any) => d.x1 + 8)
      .attr('y', (d: any) => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('font-size', 11)
      .attr('fill', 'currentColor')
      .text((d: any) => d.label || (d.type === 'table' ? d.id.split('.').slice(-1)[0] : d.id))
      .style('pointer-events', 'none')

    // Fit to view on first render
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
  }, [graph, joins, size.w, size.h])

  return (
    <div ref={wrapRef} style={{ width: '100%', height: '100%' }}>
      <svg ref={svgRef} width={size.w} height={size.h} />
    </div>
  )
}

