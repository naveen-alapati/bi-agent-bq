import React, { useEffect, useMemo, useState } from 'react'
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

  useEffect(() => {
    setLoadError('')
    api.getDatasets().then(setDatasets).catch(() => setLoadError('Failed to fetch datasets. Ensure the Cloud Run service account has BigQuery list permissions.'))
    api.listDashboards().then(setDashList).catch(() => {})
  }, [])

  async function onAnalyze() {
    if (!selected.length) return
    setLoading(true)
    try {
      await api.prepare(selected, 5)
      const kpisResp = await api.generateKpis(selected, 5)
      setKpis(kpisResp)
      // auto-store generated KPIs in catalog
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
      // categorical crossFilter wiring placeholder
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
        id: asNew ? undefined : undefined, // new id when Save As; leave undefined for auto
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
    // convert catalog item to KPI card shape
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

      <div style={{ display: 'grid', gridTemplateColumns: '340px 1fr', gap: 16, padding: 16 }}>
        <div>
          <h3>Data</h3>
          {loadError && <div style={{ color: 'crimson', marginBottom: 8 }}>{loadError}</div>}
          {datasets.length === 0 ? (
            <div style={{ color: '#666' }}>No datasets found. Verify IAM and BigQuery project.</div>
          ) : (
            <TableSelector datasets={datasets} onChange={setSelected} />
          )}
          <button onClick={onAnalyze} disabled={!selected.length || loading} style={{ marginTop: 8 }}>
            {loading ? 'Analyzing...' : `Analyze (${selected.length})`}
          </button>

          <div style={{ marginTop: 16 }}>
            <h3>KPI Catalog</h3>
            <KPICatalog onAdd={addKpiToCanvas} />
          </div>

          <div style={{ marginTop: 16 }}>
            <h3>Dashboards</h3>
            <div style={{ marginTop: 8 }}>
              <select onChange={e => api.getDashboard(e.target.value).then(d => { setDashboardName(d.name); setVersion(d.version || ''); setKpis(d.kpis); setLayouts(d.layouts?.lg || d.layout || []); setSelected(d.selected_tables); setGlobalDate(d.global_filters?.date || {}); })} style={{ width: '100%' }}>
                <option value="">Load existing...</option>
                {dashList.map(d => (
                  <option key={d.id} value={d.id}>{d.name} (v{d.version})</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        <div>
          <h3>Dashboard</h3>
          <GridLayout
            className="layout"
            layout={layouts}
            cols={12}
            rowHeight={30}
            width={1000}
            isResizable
            isDraggable
            onLayoutChange={onLayoutChange}
          >
            {kpis.map(k => (
              <div key={k.id} data-grid={layouts.find(l => l.i === k.id)} style={{ border: '1px solid #ddd', background: '#fff', display: 'flex', flexDirection: 'column' }}>
                <div style={{ padding: 8, display: 'flex', justifyContent: 'space-between', cursor: 'move' }}>
                  <div>
                    <div style={{ fontWeight: 600 }}>{k.name}</div>
                    <div style={{ color: '#666', fontSize: 12 }}>{k.short_description}</div>
                  </div>
                  <div style={{ display: 'flex', gap: 6 }}>
                    <button onClick={() => runKpi(k)} style={{ fontSize: 12 }}>Run</button>
                    <button onClick={() => window.alert(k.sql)} style={{ fontSize: 12 }}>View SQL</button>
                    <button onClick={async () => {
                      const instruction = prompt('Edit instruction (e.g., group by month, limit 12)')
                      if (!instruction) return
                      const res = await fetch('/api/sql/edit', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql, instruction }) }).then(r => r.json())
                      k.sql = res.sql
                      setKpis([...kpis])
                    }} style={{ fontSize: 12 }}>Edit SQL</button>
                    <button onClick={async () => {
                      const r = await fetch('/api/export/card', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql }) })
                      const blob = await r.blob()
                      const url = URL.createObjectURL(blob)
                      const a = document.createElement('a')
                      a.href = url
                      a.download = `${k.name || 'card'}.csv`
                      a.click()
                      URL.revokeObjectURL(url)
                    }} style={{ fontSize: 12 }}>Export</button>
                  </div>
                </div>
                <div style={{ flex: 1, padding: 8 }}>
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