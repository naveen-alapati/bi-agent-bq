import React from 'react'

type Props = {
  kpis: any[]
  onRun: (kpi: any) => void
}

export function KPIList({ kpis, onRun }: Props) {
  if (!kpis.length) return <div>No KPIs yet.</div>
  return (
    <div>
      {kpis.map(k => (
        <div key={k.id} style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid #eee', padding: '6px 0' }}>
          <div>
            <div style={{ fontWeight: 600 }}>{k.name}</div>
            <div style={{ color: '#666' }}>{k.short_description}</div>
            <div style={{ fontSize: 12, color: '#999' }}>{k.d3_chart} [{k.chart_type}]</div>
          </div>
          <button onClick={() => onRun(k)}>Run</button>
        </div>
      ))}
    </div>
  )
}