import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../services/api'
import GridLayout, { Layout } from 'react-grid-layout'
import { ChartRenderer } from '../ui/ChartRenderer'
import '../styles.css'

export default function Home() {
  const [dashboards, setDashboards] = useState<any[]>([])
  const [activeId, setActiveId] = useState<string>('')
  const [active, setActive] = useState<any | null>(null)
  const [rowsByKpi, setRowsByKpi] = useState<Record<string, any[]>>({})
  const gridWrapRef = useRef<HTMLDivElement | null>(null)
  const [gridW, setGridW] = useState<number>(1000)

  useEffect(() => { api.listDashboards().then(setDashboards).catch(() => {}) }, [])

  useEffect(() => {
    const el = gridWrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setGridW(el.clientWidth))
    ro.observe(el)
    setGridW(el.clientWidth)
    return () => ro.disconnect()
  }, [gridWrapRef.current])

  async function loadDashboard(id: string) {
    setActiveId(id)
    const d = await api.getDashboard(id)
    setActive(d)
    setRowsByKpi({})
    // Preload data for visible KPIs lightweight (optional, keep lazy to save cost)
  }

  async function runKpi(kpi: any) {
    const res = await api.runKpi(kpi.sql, undefined as any, kpi.filter_date_column, kpi.expected_schema)
    setRowsByKpi(prev => ({ ...prev, [kpi.id]: res }))
  }

  const layout: Layout[] = useMemo(() => {
    if (!active) return []
    const l = (active.layout && active.layout.length ? active.layout : (active.layouts && (active.layouts['lg'] || active.layouts['md'] || active.layouts['sm']) || [])) as Layout[]
    if (l && l.length) return l
    // fallback layout
    return (active.kpis || []).map((k: any, i: number) => ({ i: k.id, x: (i % 2) * 6, y: Math.floor(i/2) * 8, w: 6, h: 8 }))
  }, [active])

  return (
    <div>
      <div className="topbar header-gradient" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', position: 'sticky', top: 0, zIndex: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ fontSize: 18, fontWeight: 700 }}>Dashboards</div>
          {active && <span className="badge">{active.name} v{active.version}</span>}
        </div>
        <div className="toolbar">
          <a className="btn" href="/editor">New Dashboard</a>
          {active && <a className="btn btn-primary" href={`/editor`}>Edit Dashboard</a>}
        </div>
      </div>

      <div className="app-grid">
        <div className="sidebar" style={{ display: 'grid', gap: 12 }}>
          <div className="panel">
            <div className="section-title">All Dashboards</div>
            <div className="scroll">
              {dashboards.map(d => (
                <button key={d.id} className="btn" onClick={() => loadDashboard(d.id)} style={{ display: 'flex', width: '100%', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6, background: 'linear-gradient(90deg, var(--surface), rgba(122,218,165,0.2))' }}>
                  <span style={{ textAlign: 'left' }}>
                    <div className="card-title">{d.name}</div>
                    <div className="card-subtitle">v{d.version}</div>
                  </span>
                  <span className="chip">View</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gap: 12 }} ref={gridWrapRef}>
          {!active && <div className="panel"><div className="card-subtitle">Select a dashboard from the left.</div></div>}
          {active && (
            <>
              <div className="section-title">{active.name} <span className="chip">v{active.version}</span></div>
              <GridLayout
                className="layout"
                layout={layout}
                cols={12}
                rowHeight={30}
                width={gridW}
                isResizable={false}
                isDraggable={false}
              >
                {(active.kpis || []).map((k: any) => (
                  <div key={k.id} data-grid={layout.find(l => l.i === k.id)} className="card" style={{ display: 'flex', flexDirection: 'column' }}>
                    <div className="card-header">
                      <div>
                        <div className="card-title">{k.name}</div>
                        <div className="card-subtitle">{k.short_description}</div>
                      </div>
                      <div className="card-actions">
                        <button className="btn btn-sm" onClick={() => runKpi(k)}>Run</button>
                      </div>
                    </div>
                    <div style={{ flex: 1, padding: 8 }} className="no-drag">
                      <ChartRenderer chart={k} rows={rowsByKpi[k.id] || []} />
                    </div>
                  </div>
                ))}
              </GridLayout>
            </>
          )}
        </div>
      </div>
    </div>
  )
}