import React, { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'

type GraphNode = { id: string; type: 'table' | 'column' | 'output'; label?: string }
type GraphEdge = { source: string; target: string; type: 'join' | 'join_table' | 'projection' }

export function LineageGraph({ graph }: { graph: { nodes: GraphNode[]; edges: GraphEdge[] } }) {
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

    const zoom = d3.zoom<SVGSVGElement, unknown>().scaleExtent([0.2, 4]).on('zoom', (event) => {
      g.attr('transform', String(event.transform))
    })
    svg.call(zoom as any)

    const color = (d: GraphNode) => d.type === 'table' ? '#239BA7' : d.type === 'output' ? '#8B5CF6' : '#64748B'
    const radius = (d: GraphNode) => d.type === 'table' ? 16 : d.type === 'output' ? 12 : 8

    const nodes: any[] = graph.nodes.map(n => ({ ...n }))
    const nodeById = new Map(nodes.map(n => [n.id, n]))
    const links: any[] = graph.edges.map(e => ({ ...e, source: nodeById.get(e.source), target: nodeById.get(e.target) })).filter(l => l.source && l.target)

    const sim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id((d: any) => d.id).distance((l: any) => l.type === 'join_table' ? 120 : 60))
      .force('charge', d3.forceManyBody().strength(-220))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius((d: any) => radius(d) + 8))

    const link = g.append('g').attr('stroke', '#999').attr('stroke-opacity', 0.6)
      .selectAll('line')
      .data(links)
      .enter().append('line')
      .attr('stroke-width', (d: any) => d.type === 'join_table' ? 2.4 : 1.4)
      .attr('stroke-dasharray', (d: any) => d.type === 'projection' ? '4 2' : null)

    const node = g.append('g').selectAll('g').data(nodes).enter().append('g').call(d3.drag<SVGGElement, any>()
      .on('start', (event, d: any) => { if (!event.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y })
      .on('drag', (event, d: any) => { d.fx = event.x; d.fy = event.y })
      .on('end', (event, d: any) => { if (!event.active) sim.alphaTarget(0); d.fx = null; d.fy = null })
    )

    node.append('circle')
      .attr('r', (d: any) => radius(d))
      .attr('fill', (d: any) => color(d))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5)

    node.append('title').text((d: any) => d.id)

    node.append('text')
      .text((d: any) => d.label || d.id.split('.').slice(-1)[0])
      .attr('x', 10)
      .attr('y', 4)
      .attr('font-size', 11)
      .attr('fill', 'var(--fg)')

    sim.on('tick', () => {
      link
        .attr('x1', (d: any) => d.source.x)
        .attr('y1', (d: any) => d.source.y)
        .attr('x2', (d: any) => d.target.x)
        .attr('y2', (d: any) => d.target.y)

      node.attr('transform', (d: any) => `translate(${d.x},${d.y})`)
    })

    // Fit to view on first render
    setTimeout(() => {
      const bounds = g.node()?.getBBox()
      if (bounds && isFinite(bounds.width) && isFinite(bounds.height)) {
        const scale = 0.9 / Math.max(bounds.width / width, bounds.height / height)
        const translate = [width / 2 - scale * (bounds.x + bounds.width / 2), height / 2 - scale * (bounds.y + bounds.height / 2)]
        svg.transition().duration(350).call(zoom.transform as any, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale))
      }
    }, 0)
  }, [graph, size.w, size.h])

  return (
    <div ref={wrapRef} style={{ width: '100%', height: '100%' }}>
      <svg ref={svgRef} width={size.w} height={size.h} />
    </div>
  )
}

