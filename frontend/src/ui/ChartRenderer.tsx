import React, { useEffect, useRef } from 'react'
import embed, { VisualizationSpec } from 'vega-embed'
import { ChartCanvas } from './ChartCanvas'

export function ChartRenderer({ chart, rows }: { chart: any, rows: any[] }) {
  const vegaRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (chart.engine === 'vega-lite' && chart.vega_lite_spec && vegaRef.current) {
      const spec: VisualizationSpec = {
        ...(chart.vega_lite_spec as any),
        data: { values: rows || [] },
      }
      embed(vegaRef.current, spec, { actions: false }).catch(() => {})
    }
  }, [chart, rows])

  if (chart.engine === 'vega-lite' && chart.vega_lite_spec) {
    return <div ref={vegaRef} style={{ width: '100%', height: '100%' }} />
  }

  return <ChartCanvas chart={chart} rows={rows} />
}