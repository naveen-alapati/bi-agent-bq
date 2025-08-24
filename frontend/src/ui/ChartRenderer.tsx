import React, { useEffect, useRef, useState } from 'react'
import embed, { VisualizationSpec, Result } from 'vega-embed'
import { ChartCanvas } from './ChartCanvas'

export function ChartRenderer({ chart, rows, onSelect, onError }: { chart: any, rows: any[], onSelect?: (payload: any) => void, onError?: (err: { type: string; message: string; raw?: any }) => void }) {
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
        onError && onError({ type: 'VegaEmbedError', message: String(e?.message || e), raw: e })
      })
    } catch (e: any) {
      console.error('vega spec parse error', e, chart)
      onError && onError({ type: 'VegaSpecParseError', message: String(e?.message || e), raw: e })
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