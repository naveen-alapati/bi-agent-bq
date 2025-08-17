import React, { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'

type Props = {
  chart: any
  rows: any[]
}

export function ChartCanvas({ chart, rows }: Props) {
  const ref = useRef<SVGSVGElement | null>(null)
  const wrapRef = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 0, h: 0 })

  useEffect(() => {
    if (!wrapRef.current) return
    const el = wrapRef.current
    const obs = new ResizeObserver(entries => {
      for (const entry of entries) {
        const cr = entry.contentRect
        setSize({ w: Math.max(200, cr.width), h: Math.max(180, cr.height) })
      }
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [])

  useEffect(() => {
    const svg = d3.select(ref.current)
    svg.selectAll('*').remove()
    const width = size.w || 600
    const height = size.h || 280
    svg.attr('viewBox', `0 0 ${width} ${height}`)

    if (!rows || rows.length === 0) return

    if (chart.expected_schema?.startsWith('timeseries')) {
      const xVals = rows.map(r => r.x)
      const isDate = /DATE|TIMESTAMP|date|time/i.test(chart.expected_schema)
      const parse = (v: any) => isDate ? new Date(v) : v
      const xRange = [40, width - 10]
      const yRange = [height - 30, 10]
      const x = isDate ? d3.scaleUtc().domain(d3.extent(xVals.map(parse)) as any).range(xRange)
                        : d3.scalePoint().domain(xVals).range(xRange)
      const y = d3.scaleLinear().domain([0, d3.max(rows, r => +r.y) || 0]).nice().range(yRange)
      const line = d3.line<any>().x(r => (x(parse(r.x)) as number)).y(r => y(+r.y))
      svg.append('g').attr('transform', `translate(0,${height-30})`).call(d3.axisBottom(x as any))
      svg.append('g').attr('transform', 'translate(40,0)').call(d3.axisLeft(y))
      svg.append('path').datum(rows).attr('fill','none').attr('stroke','#3b82f6').attr('stroke-width',2).attr('d', line)
    } else if (chart.expected_schema?.startsWith('categorical') || chart.expected_schema?.startsWith('distribution')) {
      const labels = rows.map(r => r.label)
      const x = d3.scaleBand().domain(labels).range([40,  width-10]).padding(0.2)
      const y = d3.scaleLinear().domain([0, d3.max(rows, r => +r.value) || 0]).nice().range([height-30, 10])
      svg.append('g').attr('transform', `translate(0,${height-30})`).call(d3.axisBottom(x))
      svg.append('g').attr('transform', 'translate(40,0)').call(d3.axisLeft(y))
      svg.append('g')
        .selectAll('rect')
        .data(rows)
        .enter()
        .append('rect')
        .attr('x', d => (x(d.label) || 0))
        .attr('y', d => y(+d.value))
        .attr('width', d => x.bandwidth())
        .attr('height', d => (height-30) - y(+d.value))
        .attr('fill', '#10b981')
    } else if (chart.chart_type === 'scatter') {
      const x = d3.scaleLinear().domain([0, d3.max(rows, r => +r.x) || 0]).nice().range([40, width-10])
      const y = d3.scaleLinear().domain([0, d3.max(rows, r => +r.y) || 0]).nice().range([height-30, 10])
      svg.append('g').attr('transform', `translate(0,${height-30})`).call(d3.axisBottom(x))
      svg.append('g').attr('transform', 'translate(40,0)').call(d3.axisLeft(y))
      svg.append('g').selectAll('circle').data(rows).enter().append('circle')
        .attr('cx', d => x(+d.x))
        .attr('cy', d => y(+d.y))
        .attr('r', 3)
        .attr('fill', '#ef4444')
    } else {
      svg.append('text').attr('x', 10).attr('y', 20).text('Unsupported chart type or schema')
    }
  }, [chart, rows, size])

  return <div ref={wrapRef} className="no-drag" style={{ width: '100%', height: '100%' }}><svg ref={ref} style={{ width: '100%', height: '100%' }} /></div>
}