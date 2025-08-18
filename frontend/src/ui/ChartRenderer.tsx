import React, { useEffect, useRef, useState } from 'react'
import embed, { VisualizationSpec, Result } from 'vega-embed'
import { ChartCanvas } from './ChartCanvas'

export function ChartRenderer({ chart, rows, onSelect }: { chart: any, rows: any[], onSelect?: (payload: any) => void }) {
  const wrapRef = useRef<HTMLDivElement | null>(null)
  const vegaRef = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 0, h: 0 })

  useEffect(() => {
    if (!wrapRef.current) return
    const el = wrapRef.current
    const obs = new ResizeObserver(entries => {
      for (const entry of entries) {
        const cr = entry.contentRect
        setSize({ w: Math.max(100, cr.width), h: Math.max(100, cr.height) })
      }
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [])

  // Simple card renderer for single metric KPIs
  if ((chart.chart_type === 'card') || (chart.expected_schema && /card|single/i.test(chart.expected_schema))) {
    const fmt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 })
    const arr = Array.isArray(rows) ? rows : []
    let value: number | string | null = null
    if (arr.length) {
      const r = (arr[0] || {}) as any
      if (typeof r.value === 'number') value = r.value
      else if (typeof r.y === 'number') value = r.y
      else {
        // find first numeric field
        const numKey = Object.keys(r).find(k => typeof r[k] === 'number')
        value = typeof numKey !== 'undefined' ? r[numKey!] : null
      }
    }
    const display = value == null ? '—' : (typeof value === 'number' ? fmt.format(value) : String(value))
    const title = chart.name || 'KPI'
    return (
      <div ref={wrapRef} className="no-drag" style={{ width: '100%', height: '100%', minHeight: 160, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 14, color: 'var(--muted)', marginBottom: 6 }}>{title}</div>
          <div style={{ fontSize: Math.max(24, Math.min(64, size.w * 0.12)), fontWeight: 800, letterSpacing: '0.5px' }}>{display}</div>
        </div>
      </div>
    )
  }

  useEffect(() => {
    if (!(chart.engine === 'vega-lite' && chart.vega_lite_spec && vegaRef.current)) return
    if (size.w <= 0 || size.h <= 0) return
    let result: Result | null = null
    try {
      const baseSpec = typeof chart.vega_lite_spec === 'string' ? JSON.parse(chart.vega_lite_spec) : chart.vega_lite_spec
      const spec: VisualizationSpec = {
        ...(baseSpec as any),
        data: { values: rows || [] },
        width: 'container' as any,
        height: 'container' as any,
        autosize: { type: 'fit', contains: 'padding', resize: true } as any,
      }
      embed(vegaRef.current!, spec, { actions: false, renderer: 'canvas' }).then((res) => {
        result = res
        const view = res.view
        view.resize()
        view.addEventListener('click', (_evt: any, item: any) => {
          if (!item || !item.datum) return
          onSelect && onSelect({ datum: item.datum, chart })
        })
      }).catch((e) => {
        console.error('vega-embed error', e, chart)
      })
    } catch (e) {
      console.error('vega spec parse error', e, chart)
    }
    return () => {
      try { result && result.view && result.view.finalize && result.view.finalize() } catch {}
    }
  }, [chart, rows, size])

  if (chart.engine === 'vega-lite' && chart.vega_lite_spec) {
    return <div ref={wrapRef} className="no-drag" style={{ width: '100%', height: '100%', minHeight: 200 }}>
      <div ref={vegaRef} style={{ width: '100%', height: '100%' }} />
    </div>
  }

  return <ChartCanvas chart={chart} rows={rows} />
}