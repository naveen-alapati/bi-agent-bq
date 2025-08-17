import React, { useEffect, useMemo, useState, useRef } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { TableSelector } from '../ui/TableSelector'
import { KPIList } from '../ui/KPIList'
import { ChartRenderer } from '../ui/ChartRenderer'
import { api } from '../services/api'
import '../styles.css'
import GridLayout, { Layout } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import { TopBar } from '../ui/TopBar'
import { KPICatalog } from '../ui/KPICatalog'

export default function App() {
  const params = useParams()
  const [search] = useSearchParams()
  const routeId = params.id || search.get('dashboardId') || ''
  const [datasets, setDatasets] = useState<any[]>([])
  const [selected, setSelected] = useState<{datasetId: string, tableId: string}[]>([])
  const [kpis, setKpis] = useState<any[]>([])
  const [rowsByKpi, setRowsByKpi] = useState<Record<string, any[]>>({})
  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState('')
  const [dashboardName, setDashboardName] = useState('ecom-v1')
  const [version, setVersion] = useState<string>('')
  const [layouts, setLayouts] = useState<Layout[]>([])
  const [dashList, setDashList] = useState<any[]>([])
  const [saving, setSaving] = useState(false)
  const [globalDate, setGlobalDate] = useState<{from?: string, to?: string}>({})
  const [crossFilter, setCrossFilter] = useState<any>(null)
  const [theme, setTheme] = useState<'light' | 'dark'>('light')
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true)
  const gridWrapRef = useRef<HTMLDivElement | null>(null)
  const [gridW, setGridW] = useState<number>(1000)
  const [toasts, setToasts] = useState<{ id: number; type: 'success'|'error'; msg: string }[]>([])
  const toast = (type: 'success'|'error', msg: string) => {
    const id = Date.now() + Math.random()
    setToasts(t => [...t, { id, type, msg }])
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 4500)
  }
  const [tabs, setTabs] = useState<{ id: string; name: string; order: number }[]>([{ id: 'overview', name: 'Overview', order: 0 }])
  const [tabLayouts, setTabLayouts] = useState<Record<string, Layout[]>>({ overview: [] })
  const [activeTab, setActiveTab] = useState<string>('overview')
  const [editingTabId, setEditingTabId] = useState<string | null>(null)
  const [editingTabName, setEditingTabName] = useState<string>('')
  const [dragTabId, setDragTabId] = useState<string | null>(null)
  const defaultPalette = { primary: '#239BA7', accent: '#7ADAA5', surface: '#ECECBB', warn: '#E1AA36' }
  const [palette, setPalette] = useState<{ primary: string; accent: string; surface: string; warn: string }>(defaultPalette)
  const [dirty, setDirty] = useState<boolean>(false)

  function applyPalette(p: { primary: string; accent: string; surface: string; warn: string }) {
    const r = document.documentElement
    r.style.setProperty('--primary', p.primary)
    r.style.setProperty('--accent', p.accent)
    r.style.setProperty('--surface', p.surface)
    r.style.setProperty('--warn', p.warn)
  }

  useEffect(() => { applyPalette(palette) }, [])

  const tabColors = [palette.primary, palette.accent, palette.warn, palette.surface]
  const colorForTab = (id: string, idx: number) => tabColors[idx % tabColors.length]

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  useEffect(() => {
    setLoadError('')
    api.getDatasets().then(setDatasets).catch(() => setLoadError('Failed to fetch datasets. Ensure the Cloud Run service account has BigQuery list permissions.'))
    api.listDashboards().then(setDashList).catch(() => {})
  }, [])

  useEffect(() => {
    if (!routeId) return
    api.getDashboard(routeId).then(d => {
      setDashboardName(d.name)
      setVersion(d.version || '')
      setKpis(d.kpis.map((k:any) => ({ ...k, tabs: Array.isArray(k.tabs) && k.tabs.length ? k.tabs : ['overview'] })))
      const nextTabs = (d.tabs && d.tabs.length ? d.tabs : [{ id: 'overview', name: 'Overview', order: 0 }])
      setTabs(nextTabs)
      const tl = d.tab_layouts || {}
      if (!tl['overview'] && Array.isArray(d.layout)) tl['overview'] = d.layout
      setTabLayouts(tl)
      setActiveTab(d.last_active_tab || 'overview')
      setSelected(d.selected_tables)
      setGlobalDate((d.global_filters && d.global_filters.date) || {})
      const mode = (d.theme && (d.theme.mode as any)) || 'light'
      setTheme(mode === 'dark' ? 'dark' : 'light')
      const savedPal = (d.theme && (d.theme.palette as any)) || null
      if (savedPal && savedPal.primary) { setPalette(savedPal); applyPalette(savedPal) } else { applyPalette(palette) }
      setDirty(false)
    }).catch(() => {})
  }, [routeId])

  useEffect(() => {
    const el = gridWrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setGridW(el.clientWidth))
    ro.observe(el)
    setGridW(el.clientWidth)
    return () => ro.disconnect()
  }, [gridWrapRef.current])

  async function onAnalyze() {
    if (!selected.length) return
    setLoading(true)
    try {
      await api.prepare(selected, 5)
      const kpisResp = await api.generateKpis(selected, 5)
      setKpis(kpisResp)
      for (const sel of selected) {
        const perTable = kpisResp.filter(k => (k.id || '').startsWith(`${sel.datasetId}.${sel.tableId}:`))
        if (perTable.length) {
          await api.addToKpiCatalog(sel.datasetId, sel.tableId, perTable)
        }
      }
    } finally {
      setLoading(false)
    }
  }

  async function runKpi(kpi: any) {
    const filters = {
      date: globalDate,
    }
    const res = await api.runKpi(kpi.sql, filters, kpi.filter_date_column, kpi.expected_schema)
    setRowsByKpi(prev => ({...prev, [kpi.id]: res}))
  }

  useEffect(() => {
    const defaultLayout = kpis.map((k, i) => ({ i: k.id, x: (i % 2) * 6, y: Math.floor(i / 2) * 8, w: 6, h: 8 }))
    setLayouts(defaultLayout)
    setTabLayouts(prev => ({ ...prev, [activeTab]: defaultLayout }))
  }, [kpis])

  function onLayoutChange(newLayout: Layout[]) {
    setLayouts(newLayout)
    setTabLayouts(prev => ({ ...prev, [activeTab]: newLayout }))
    setDirty(true)
  }

  async function saveDashboard(asNew?: boolean) {
    setSaving(true)
    try {
      const payload = {
        id: asNew ? undefined : undefined,
        name: dashboardName,
        version: asNew ? '1.0.0' : undefined,
        kpis,
        layout: layouts,
        layouts: undefined,
        selected_tables: selected,
        global_filters: { date: globalDate },
        theme: { mode: theme, palette },
        tabs,
        tab_layouts: tabLayouts,
        last_active_tab: activeTab,
      }
      const res = await api.saveDashboard(payload as any)
      setVersion(res.version)
      await api.listDashboards().then(setDashList)
      toast('success', `Saved ${res.name} v${res.version}`)
      setDirty(false)
    } catch (e: any) {
      toast('error', e?.message || 'Failed to save')
    } finally {
      setSaving(false)
    }
  }

  async function exportDashboard() {
    await fetch('/api/export/dashboard', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ kpis }) })
      .then(async r => ({ blob: await r.blob() }))
      .then(({ blob }) => {
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${dashboardName || 'dashboard'}.zip`
        a.click()
        URL.revokeObjectURL(url)
      })
  }

  function addKpiToCanvas(item: any) {
    const id = `${item.dataset_id}.${item.table_id}:${item.id}`
    const k: any = {
      id,
      name: item.name,
      short_description: '',
      chart_type: item.chart_type,
      d3_chart: '',
      expected_schema: item.expected_schema,
      sql: item.sql,
      engine: item.engine,
      vega_lite_spec: item.vega_lite_spec,
    }
    setKpis(prev => [...prev, k])
    setLayouts(prev => [...prev, { i: id, x: 0, y: Infinity, w: 6, h: 8 }])
  }

  function addTab() {
    const name = prompt('New tab name') || ''
    if (!name) return
    const id = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || `tab-${Date.now()}`
    const order = tabs.length
    const next = [...tabs, { id, name, order }]
    setTabs(next)
    setActiveTab(id)
    setTabLayouts(prev => ({ ...prev, [id]: [] }))
  }

  function removeTab(id: string) {
    if (id === 'overview') return
    const next = tabs.filter(t => t.id !== id)
    setTabs(next.map((t, i) => ({ ...t, order: i })))
    setActiveTab('overview')
    const copy = { ...tabLayouts }
    delete copy[id]
    setTabLayouts(copy)
  }

  function toggleKpiTab(k: any, tabId: string) {
    const idx = kpis.findIndex(x => x.id === k.id)
    if (idx < 0) return
    const current = kpis[idx]
    const set = new Set(current.tabs || ['overview'])
    if (set.has(tabId)) set.delete(tabId)
    else set.add(tabId)
    const next = [...kpis]
    next[idx] = { ...current, tabs: Array.from(set) }
    setKpis(next)
  }

  function startEditTab(t: {id: string; name: string}) {
    setEditingTabId(t.id)
    setEditingTabName(t.name)
  }

  function commitEditTab() {
    if (!editingTabId) return
    const name = (editingTabName || '').trim() || '(untitled)'
    setTabs(prev => prev.map(tab => tab.id === editingTabId ? { ...tab, name } : tab))
    setEditingTabId(null)
    setEditingTabName('')
  }

  function cancelEditTab() {
    setEditingTabId(null)
    setEditingTabName('')
  }

  function onDragStartTab(id: string, e: React.DragEvent) {
    setDragTabId(id)
    try { e.dataTransfer.setData('text/plain', id) } catch {}
  }

  function onDragOverTab(e: React.DragEvent) { e.preventDefault() }

  function onDropTab(targetId: string, e: React.DragEvent) {
    e.preventDefault()
    const srcId = dragTabId || (() => { try { return e.dataTransfer.getData('text/plain') } catch { return '' } })()
    if (!srcId || srcId === targetId) { setDragTabId(null); return }
    setTabs(prev => {
      const arr = [...prev]
      const srcIdx = arr.findIndex(t => t.id === srcId)
      const tgtIdx = arr.findIndex(t => t.id === targetId)
      if (srcIdx === -1 || tgtIdx === -1) return prev
      const [moved] = arr.splice(srcIdx, 1)
      arr.splice(tgtIdx, 0, moved)
      return arr.map((t, i) => ({ ...t, order: i }))
    })
    setDragTabId(null)
  }

  const visibleKpis = kpis.filter(k => (k.tabs && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview'))
  const activeLayout = tabLayouts[activeTab] || layouts

  return (
    <div>
      <div className="toast-container">
        {toasts.map(t => (<div key={t.id} className={`toast ${t.type==='success'?'toast-success':'toast-error'}`}>{t.msg}</div>))}
      </div>
      <TopBar
        name={dashboardName}
        version={version}
        onNameChange={(v) => { setDashboardName(v); setDirty(true) }}
        onSave={() => saveDashboard(false)}
        onSaveAs={() => saveDashboard(true)}
        globalDate={globalDate}
        onGlobalDateChange={(v) => { setGlobalDate(v); setDirty(true) }}
        theme={theme}
        onThemeToggle={() => setTheme(t => (t === 'light' ? 'dark' : 'light'))}
        onExportDashboard={exportDashboard}
        onToggleSidebar={() => setSidebarOpen(o => !o)}
        sidebarOpen={sidebarOpen}
        dirty={dirty}
      />

      <div className={`app-grid ${!sidebarOpen ? 'app-grid--no-sidebar' : ''}`}>
        {sidebarOpen && (
          <div className="sidebar" style={{ display: 'grid', gap: 12 }}>
            <div className="panel">
              <div className="section-title">Theme</div>
              <div className="toolbar">
                <div>
                  <label className="card-subtitle">Primary</label>
                  <input className="input" type="color" value={palette.primary} onChange={e => { const p = { ...palette, primary: e.target.value }; setPalette(p); applyPalette(p); setDirty(true) }} />
                </div>
                <div>
                  <label className="card-subtitle">Accent</label>
                  <input className="input" type="color" value={palette.accent} onChange={e => { const p = { ...palette, accent: e.target.value }; setPalette(p); applyPalette(p); setDirty(true) }} />
                </div>
                <div>
                  <label className="card-subtitle">Surface</label>
                  <input className="input" type="color" value={palette.surface} onChange={e => { const p = { ...palette, surface: e.target.value }; setPalette(p); applyPalette(p); setDirty(true) }} />
                </div>
                <div>
                  <label className="card-subtitle">Warn</label>
                  <input className="input" type="color" value={palette.warn} onChange={e => { const p = { ...palette, warn: e.target.value }; setPalette(p); applyPalette(p); setDirty(true) }} />
                </div>
                <button className="btn btn-sm" onClick={() => { setPalette(defaultPalette); applyPalette(defaultPalette); setDirty(true) }}>Reset Palette</button>
              </div>
            </div>
            <div className="panel">
              <div className="section-title">Data</div>
              {loadError && <div className="badge" style={{ marginBottom: 8, borderColor: 'crimson', color: 'crimson', background: 'rgba(220,20,60,0.06)' }}>{loadError}</div>}
              {datasets.length === 0 ? (
                <div className="card-subtitle">No datasets found. Verify IAM and BigQuery project.</div>
              ) : (
                <TableSelector datasets={datasets} onChange={setSelected} />
              )}
              <button className="btn btn-primary" onClick={onAnalyze} disabled={!selected.length || loading} style={{ marginTop: 8 }}>
                {loading ? 'Analyzing...' : `Analyze (${selected.length})`}
              </button>
            </div>
            <div className="panel">
              <div className="section-title">KPI Catalog</div>
              <KPICatalog onAdd={addKpiToCanvas} />
            </div>
            <div className="panel">
              <div className="section-title">Dashboards</div>
              <div style={{ marginTop: 8 }}>
                <select className="select" onChange={e => {
                  const id = e.target.value
                  if (!id) return
                  api.getDashboard(id).then(d => {
                    setDashboardName(d.name)
                    setVersion(d.version || '')
                    setKpis(d.kpis.map((k:any) => ({ ...k, tabs: Array.isArray(k.tabs) && k.tabs.length ? k.tabs : ['overview'] })))
                    const nextTabs = (d.tabs && d.tabs.length ? d.tabs : [{ id: 'overview', name: 'Overview', order: 0 }])
                    setTabs(nextTabs)
                    const tl = d.tab_layouts || {}
                    if (!tl['overview'] && Array.isArray(d.layout)) tl['overview'] = d.layout
                    setTabLayouts(tl)
                    setActiveTab(d.last_active_tab || 'overview')
                    setSelected(d.selected_tables)
                    setGlobalDate((d.global_filters && d.global_filters.date) || {})
                    const mode = (d.theme && (d.theme.mode as any)) || 'light'
                    setTheme(mode === 'dark' ? 'dark' : 'light')
                    const savedPal = (d.theme && (d.theme.palette as any)) || null
                    if (savedPal && savedPal.primary) { setPalette(savedPal); applyPalette(savedPal) } else { applyPalette(palette) }
                    setDirty(false)
                  })
                }}>
                  <option value="">Load existing...</option>
                  {dashList.map(d => (
                    <option key={d.id} value={d.id}>{d.name} (v{d.version})</option>
                  ))}
                </select>
              </div>
            </div>
            <div className="panel">
              <div className="section-title">Tabs</div>
              <div className="toolbar" style={{ marginBottom: 8 }}>
                {tabs.sort((a,b)=>a.order-b.order).map((t, idx) => (
                  <button
                    key={t.id}
                    className="btn btn-sm"
                    style={{ background: t.id===activeTab? colorForTab(t.id, idx):'', color: t.id===activeTab? '#0b1220': undefined, borderColor: t.id===activeTab? colorForTab(t.id, idx):'' }}
                    onClick={() => setActiveTab(t.id)}
                    draggable
                    onDragStart={(e) => onDragStartTab(t.id, e)}
                    onDragOver={onDragOverTab}
                    onDrop={(e) => onDropTab(t.id, e)}
                    onDoubleClick={() => startEditTab(t)}
                  >
                    {editingTabId === t.id ? (
                      <input
                        className="input"
                        value={editingTabName}
                        onChange={e => setEditingTabName(e.target.value)}
                        autoFocus
                        onBlur={commitEditTab}
                        onKeyDown={e => { if (e.key === 'Enter') commitEditTab(); if (e.key === 'Escape') cancelEditTab() }}
                        onClick={e => e.stopPropagation()}
                        style={{ maxWidth: 120 }}
                      />
                    ) : (
                      <span>{t.name}</span>
                    )}
                  </button>
                ))}
                <button className="btn btn-sm" onClick={addTab}>+ Tab</button>
                {activeTab !== 'overview' && <button className="btn btn-sm" onClick={() => { removeTab(activeTab); setDirty(true) }}>Delete Tab</button>}
              </div>
              <div className="scroll">
                {visibleKpis.map(k => (
                  <label key={k.id} className="list-item" style={{ gap: 8 }}>
                    <span style={{ flex: 1 }}>{k.name}</span>
                    <button className="btn btn-sm" onClick={() => toggleKpiTab(k, activeTab)}>{(k.tabs||[]).includes(activeTab)? 'Remove from Tab':'Add to Tab'}</button>
                  </label>
                ))}
              </div>
            </div>
          </div>
        )}

        <div style={{ display: 'grid', gap: 12 }} ref={gridWrapRef}>
          <div className="section-title">Dashboard
            {version && <span className="chip" style={{ marginLeft: 8 }}>v{version}</span>}
            <button className="btn btn-sm" style={{ marginLeft: 'auto', background: '#239BA7', color: '#fff', borderColor: '#239BA7' }} onClick={async () => {
              try {
                const list = await api.listDashboards()
                const current = list.find((d:any) => d.name === dashboardName && d.version === version)
                if (current?.id) {
                  await api.setDefaultDashboard(current.id)
                  toast('success', 'Set as default dashboard')
                } else {
                  toast('error', 'Save the dashboard first to set as default')
                }
              } catch (e:any) {
                toast('error', e?.message || 'Failed to set default')
              }
            }}>Default</button>
          </div>
          <div className="toolbar" style={{ marginBottom: 8 }}>
            {tabs.sort((a,b)=>a.order-b.order).map((t, idx) => (
              <button
                key={t.id}
                className="btn btn-sm"
                style={{ background: t.id===activeTab? colorForTab(t.id, idx):'', color: t.id===activeTab? '#0b1220': undefined, borderColor: t.id===activeTab? colorForTab(t.id, idx):'' }}
                onClick={() => setActiveTab(t.id)}
                draggable
                onDragStart={(e) => onDragStartTab(t.id, e)}
                onDragOver={onDragOverTab}
                onDrop={(e) => onDropTab(t.id, e)}
                onDoubleClick={() => startEditTab(t)}
              >
                {editingTabId === t.id ? (
                  <input
                    className="input"
                    value={editingTabName}
                    onChange={e => setEditingTabName(e.target.value)}
                    autoFocus
                    onBlur={commitEditTab}
                    onKeyDown={e => { if (e.key === 'Enter') commitEditTab(); if (e.key === 'Escape') cancelEditTab() }}
                    onClick={e => e.stopPropagation()}
                    style={{ maxWidth: 120 }}
                  />
                ) : (
                  <span>{t.name}</span>
                )}
              </button>
            ))}
            <button className="btn btn-sm" onClick={addTab}>+ Tab</button>
            {activeTab !== 'overview' && <button className="btn btn-sm" onClick={() => { removeTab(activeTab); setDirty(true) }}>Delete Tab</button>}
          </div>
          <GridLayout className="layout" layout={activeLayout} cols={12} rowHeight={30} width={gridW} isResizable isDraggable draggableHandle=".drag-handle" draggableCancel=".no-drag, button, input, textarea, select" onLayoutChange={onLayoutChange}>
            {visibleKpis.map(k => (
              <div key={k.id} data-grid={(activeLayout||[]).find(l => l.i === k.id)} className="card" style={{ display: 'flex', flexDirection: 'column' }}>
                <div className="card-header">
                  <div className="drag-handle"><div className="card-title">{k.name}</div><div className="card-subtitle">{k.short_description}</div></div>
                  <div className="card-actions no-drag">
                    <button className="btn btn-sm" onClick={() => runKpi(k)}>Test</button>
                    <button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View SQL</button>
                    <button className="btn btn-sm" onClick={async () => {
                      const instruction = prompt('AI Edit: describe the change')
                      if (!instruction) return
                      const updated = await api.editKpi(k, instruction)
                      const idx = kpis.findIndex(x => x.id === k.id)
                      if (idx >= 0) {
                        const next = [...kpis]; next[idx] = { ...next[idx], ...updated }; setKpis(next); setDirty(true)
                        try { const [ds,tb] = (k.id||'').split(':')[0].split('.'); if (ds&&tb) await api.addToKpiCatalog(ds,tb,[next[idx]]) } catch {}
                      }
                    }}>AI Edit</button>
                    <button className="btn btn-sm" onClick={async () => {
                      const r = await fetch('/api/export/card', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql }) }); const blob = await r.blob(); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `${k.name||'card'}.csv`; a.click(); URL.revokeObjectURL(url)
                    }}>Export</button>
                    <button className="btn btn-sm" onClick={() => { const nextKpis = kpis.filter(x => x.id !== k.id); const nextLayout = (activeLayout||[]).filter(l => l.i !== k.id); setKpis(nextKpis); setLayouts(nextLayout); setTabLayouts(prev => ({ ...prev, [activeTab]: nextLayout })); setDirty(true) }}>Remove</button>
                  </div>
                </div>
                <div style={{ flex: 1, padding: 8 }} className="no-drag"><ChartRenderer chart={k} rows={rowsByKpi[k.id] || []} onSelect={(p) => setCrossFilter(p)} /></div>
              </div>
            ))}
          </GridLayout>
        </div>
      </div>
    </div>
  )
}