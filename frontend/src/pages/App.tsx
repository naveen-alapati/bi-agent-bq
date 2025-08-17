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
  const gridWrapRef = useRef<HTMLDivElement | null>(null)
  const [gridW, setGridW] = useState<number>(1000)

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
      setKpis(d.kpis)
      const nextLayout = (d.layout && d.layout.length ? d.layout : (d.layouts && (d.layouts['lg'] || d.layouts['md'] || d.layouts['sm']) || [])) as Layout[]
      setLayouts(nextLayout)
      setSelected(d.selected_tables)
      setGlobalDate((d.global_filters && d.global_filters.date) || {})
      const mode = (d.theme && (d.theme.mode as any)) || 'light'
      setTheme(mode === 'dark' ? 'dark' : 'light')
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
  }, [kpis])

  function onLayoutChange(newLayout: Layout[]) {
    setLayouts(newLayout)
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
        selected_tables: selected,
        global_filters: { date: globalDate },
        theme: { mode: theme },
      }
      const res = await api.saveDashboard(payload as any)
      setVersion(res.version)
      await api.listDashboards().then(setDashList)
      alert(`Saved ${res.name} v${res.version}`)
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

  return (
    <div>
      <TopBar
        name={dashboardName}
        version={version}
        onNameChange={setDashboardName}
        onSave={() => saveDashboard(false)}
        onSaveAs={() => saveDashboard(true)}
        globalDate={globalDate}
        onGlobalDateChange={setGlobalDate}
        theme={theme}
        onThemeToggle={() => setTheme(t => (t === 'light' ? 'dark' : 'light'))}
        onExportDashboard={exportDashboard}
      />

      <div className="app-grid">
        <div className="sidebar" style={{ display: 'grid', gap: 12 }}>
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
                  setKpis(d.kpis)
                  const nextLayout = (d.layout && d.layout.length ? d.layout : (d.layouts && (d.layouts['lg'] || d.layouts['md'] || d.layouts['sm']) || [])) as Layout[]
                  setLayouts(nextLayout)
                  setSelected(d.selected_tables)
                  setGlobalDate((d.global_filters && d.global_filters.date) || {})
                  const mode = (d.theme && (d.theme.mode as any)) || 'light'
                  setTheme(mode === 'dark' ? 'dark' : 'light')
                })
              }}>
                <option value="">Load existing...</option>
                {dashList.map(d => (
                  <option key={d.id} value={d.id}>{d.name} (v{d.version})</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gap: 12 }} ref={gridWrapRef}>
          <div className="section-title">Dashboard</div>
          <GridLayout
            className="layout"
            layout={layouts}
            cols={12}
            rowHeight={30}
            width={gridW}
            isResizable
            isDraggable
            draggableHandle=".drag-handle"
            draggableCancel=".no-drag, button, input, textarea, select"
            onLayoutChange={onLayoutChange}
          >
            {kpis.map(k => (
              <div key={k.id} data-grid={layouts.find(l => l.i === k.id)} className="card" style={{ display: 'flex', flexDirection: 'column' }}>
                <div className="card-header">
                  <div className="drag-handle">
                    <div className="card-title">{k.name}</div>
                    <div className="card-subtitle">{k.short_description}</div>
                  </div>
                  <div className="card-actions no-drag">
                    <button className="btn btn-sm" onClick={() => runKpi(k)}>Run</button>
                    <button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View SQL</button>
                    <button className="btn btn-sm" onClick={async () => {
                      const instruction = prompt('Edit instruction (e.g., group by month, limit 12)')
                      if (!instruction) return
                      const res = await fetch('/api/sql/edit', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql, instruction }) }).then(r => r.json())
                      k.sql = res.sql
                      setKpis([...kpis])
                    }}>Edit SQL</button>
                    <button className="btn btn-sm" onClick={async () => {
                      const r = await fetch('/api/export/card', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql }) })
                      const blob = await r.blob()
                      const url = URL.createObjectURL(blob)
                      const a = document.createElement('a')
                      a.href = url
                      a.download = `${k.name || 'card'}.csv`
                      a.click()
                      URL.revokeObjectURL(url)
                    }}>Export</button>
                    <button className="btn btn-sm" onClick={() => {
                      const nextKpis = kpis.filter(x => x.id !== k.id)
                      const nextLayout = layouts.filter(l => l.i !== k.id)
                      setKpis(nextKpis)
                      setLayouts(nextLayout)
                      setRowsByKpi(prev => { const { [k.id]: _, ...rest } = prev; return rest })
                    }}>Remove</button>
                  </div>
                </div>
                <div style={{ flex: 1, padding: 8 }} className="no-drag">
                  <ChartRenderer chart={k} rows={rowsByKpi[k.id] || []} onSelect={(p) => setCrossFilter(p)} />
                </div>
              </div>
            ))}
          </GridLayout>
        </div>
      </div>
    </div>
  )
}