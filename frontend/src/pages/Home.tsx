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
  const [localFilters, setLocalFilters] = useState<{ from?: string; to?: string; category?: { column?: string; value?: string } }>({})
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true)
  const [toasts, setToasts] = useState<{ id: number; type: 'success'|'error'; msg: string }[]>([])
  const toast = (type: 'success'|'error', msg: string) => {
    const id = Date.now() + Math.random()
    setToasts(t => [...t, { id, type, msg }])
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 4500)
  }
  const gridWrapRef = useRef<HTMLDivElement | null>(null)
  const [gridW, setGridW] = useState<number>(1000)
  const [tabs, setTabs] = useState<{ id: string; name: string; order: number }[]>([{ id: 'overview', name: 'Overview', order: 0 }])
  const [activeTab, setActiveTab] = useState<string>('overview')

  useEffect(() => { api.listDashboards().then(setDashboards).catch(() => {}) }, [])
  useEffect(() => {
    (async () => {
      const def = await api.getDefaultDashboard().catch(() => null)
      if (def) loadDashboard(def)
    })()
  }, [])

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
    setTabs((d.tabs && d.tabs.length ? d.tabs : [{ id: 'overview', name: 'Overview', order: 0 }]))
    setActiveTab(d.last_active_tab || 'overview')
    if (d.global_filters && d.global_filters.date) { setLocalFilters({ from: d.global_filters.date.from, to: d.global_filters.date.to }) } else { setLocalFilters({}) }
    setTimeout(() => refreshAll(d), 0)
  }

  async function runKpiWithFilters(kpi: any) {
    const filters: any = {}
    if (localFilters.from || localFilters.to) filters.date = { from: localFilters.from, to: localFilters.to }
    if (localFilters.category && localFilters.category.column && localFilters.category.value) filters.category = localFilters.category
    const res = await api.runKpi(kpi.sql, filters, kpi.filter_date_column, kpi.expected_schema)
    setRowsByKpi(prev => ({ ...prev, [kpi.id]: res }))
  }

  async function refreshAll(d?: any) {
    const dash = d || active
    if (!dash) return
    for (const k of dash.kpis || []) {
      await runKpiWithFilters(k)
    }
    toast('success', 'Dashboard refreshed')
  }

  const layout: Layout[] = useMemo(() => {
    if (!active) return []
    const tl = (active.tab_layouts || {})
    return tl[activeTab] || (active.layout || [])
  }, [active, activeTab])

  const visibleKpis = useMemo(() => {
    if (!active) return []
    return (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview'))
  }, [active, activeTab])

  return (
    <div>
      <div className="toast-container">
        {toasts.map(t => (<div key={t.id} className={`toast ${t.type==='success'?'toast-success':'toast-error'}`}>{t.msg}</div>))}
      </div>
      <div className="topbar header-gradient" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', position: 'sticky', top: 0, zIndex: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button className="btn btn-ghost" onClick={() => setSidebarOpen(o => !o)} title={sidebarOpen ? 'Collapse' : 'Expand'}>|||</button>
          <div style={{ fontSize: 18, fontWeight: 700 }}>Dashboards</div>
          {active && <span className="badge">{active.name} v{active.version}</span>}
        </div>
        <div className="toolbar">
          <a className="btn" href="/editor">New Dashboard</a>
          {active && <a className="btn btn-primary" href={`/editor/${active.id}`}>Edit Dashboard</a>}
        </div>
      </div>

      <div className={`app-grid ${!sidebarOpen ? 'app-grid--no-sidebar' : ''}`}>
        {sidebarOpen && (
          <div className="sidebar" style={{ display: 'grid', gap: 12 }}>
            <div className="panel">
              <div className="section-title">All Dashboards</div>
              <div className="scroll">
                {dashboards.map(d => (
                  <button key={d.id} className="btn" onClick={() => loadDashboard(d.id)} style={{ display: 'flex', width: '100%', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6, background: d.id === active?.id ? 'linear-gradient(90deg, rgba(35,155,167,0.25), rgba(122,218,165,0.25))' : 'linear-gradient(90deg, var(--surface), rgba(122,218,165,0.2))', borderColor: d.id === active?.id ? 'var(--primary)' : undefined }}>
                    <span style={{ textAlign: 'left' }}>
                      <div className="card-title">{d.name}</div>
                      <div className="card-subtitle">v{d.version}</div>
                    </span>
                    {d.id === active?.id ? <span className="chip">Selected</span> : <span className="chip">View</span>}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}

        <div style={{ display: 'grid', gap: 12 }} ref={gridWrapRef}>
          {!active && <div className="panel"><div className="card-subtitle">Select a dashboard from the left.</div></div>}
          {active && (
            <>
              <div className="panel" style={{ display: 'flex', alignItems: 'flex-end', gap: 12, flexWrap: 'wrap' }}>
                <div>
                  <label style={{ fontSize: 12, color: 'var(--muted)', display: 'block' }}>Date</label>
                  <div>
                    <input className="input" type="date" value={localFilters.from || ''} onChange={e => setLocalFilters(f => ({ ...f, from: e.target.value }))} />
                    <span style={{ margin: '0 6px' }}>to</span>
                    <input className="input" type="date" value={localFilters.to || ''} onChange={e => setLocalFilters(f => ({ ...f, to: e.target.value }))} />
                  </div>
                </div>
                <div>
                  <label style={{ fontSize: 12, color: 'var(--muted)', display: 'block' }}>Category</label>
                  <div>
                    <input className="input" placeholder="column" value={localFilters.category?.column || ''} onChange={e => setLocalFilters(f => ({ ...f, category: { ...(f.category||{}), column: e.target.value } }))} />
                    <input className="input" placeholder="value" value={localFilters.category?.value || ''} onChange={e => setLocalFilters(f => ({ ...f, category: { ...(f.category||{}), value: e.target.value } }))} />
                  </div>
                </div>
                <button className="btn btn-primary" onClick={() => refreshAll()}>Refresh</button>
              </div>

              <div className="toolbar" style={{ gap: 6 }}>
                {tabs.sort((a,b)=>a.order-b.order).map(t => (
                  <button key={t.id} className="btn btn-sm" style={{ background: t.id===activeTab? 'var(--primary)':'', color: t.id===activeTab? '#fff': undefined, borderColor: t.id===activeTab? 'var(--primary)':'' }} onClick={() => setActiveTab(t.id)}>{t.name}</button>
                ))}
              </div>

              <GridLayout
                className="layout"
                layout={layout}
                cols={12}
                rowHeight={30}
                width={gridW}
                isResizable={false}
                isDraggable={false}
              >
                {visibleKpis.map((k: any) => (
                  <div key={k.id} data-grid={layout.find(l => l.i === k.id)} className="card" style={{ display: 'flex', flexDirection: 'column' }}>
                    <div className="card-header">
                      <div>
                        <div className="card-title">{k.name}</div>
                        <div className="card-subtitle">{k.short_description}</div>
                      </div>
                      <div className="card-actions">
                        <button className="btn btn-sm" onClick={() => runKpiWithFilters(k)}>Refresh</button>
                      </div>
                    </div>
                    <div style={{ flex: 1, padding: 8 }} className="no-drag"><ChartRenderer chart={k} rows={rowsByKpi[k.id] || []} /></div>
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