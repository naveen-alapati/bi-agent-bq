import React, { useEffect, useRef } from 'react'
import embed, { VisualizationSpec } from 'vega-embed'
import { ChartCanvas } from './ChartCanvas'

export function ChartRenderer({ chart, rows, onSelect }: { chart: any, rows: any[], onSelect?: (payload: any) => void }) {
  const vegaRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (chart.engine === 'vega-lite' && chart.vega_lite_spec && vegaRef.current) {
      try {
        const baseSpec = typeof chart.vega_lite_spec === 'string' ? JSON.parse(chart.vega_lite_spec) : chart.vega_lite_spec
        const spec: VisualizationSpec = {
          ...(baseSpec as any),
          data: { values: rows || [] },
          autosize: { type: 'fit', contains: 'padding' },
        }
        embed(vegaRef.current, spec, { actions: false, renderer: 'canvas' }).then((res) => {
          const view = res.view
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
    }
  }, [chart, rows])

  if (chart.engine === 'vega-lite' && chart.vega_lite_spec) {
    return <div ref={vegaRef} className="no-drag" style={{ width: '100%', height: '100%', display: 'flex' }} />
  }

  return <ChartCanvas chart={chart} rows={rows} />
}