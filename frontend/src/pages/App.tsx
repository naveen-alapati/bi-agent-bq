import React, { useEffect, useMemo, useState, useRef } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'
import { TableSelector } from '../ui/TableSelector'
import { KPIList } from '../ui/KPIList'
import { ChartRenderer } from '../ui/ChartRenderer'
import { api, setRetrievalAssistEnabled } from '../services/api'
import '../styles.css'
import GridLayout, { Layout } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import { TopBar } from '../ui/TopBar'
import { KPICatalog } from '../ui/KPICatalog'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { computeKpiLineage, Lineage } from '../utils/lineage'

export default function App() {
  const params = useParams()
  const [search] = useSearchParams()
  const navigate = useNavigate()
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
  const [aiEditOpen, setAiEditOpen] = useState(false)
  const [aiEditKpi, setAiEditKpi] = useState<any>(null)
  const [aiChat, setAiChat] = useState<{ role: 'assistant'|'user'; text: string }[]>([])
  const [aiInput, setAiInput] = useState('')
  const [aiTyping, setAiTyping] = useState(false)
  const [catalogRefreshKey, setCatalogRefreshKey] = useState(0)
  const [aiModalPosition, setAiModalPosition] = useState({ x: 0, y: 0 })
  const [isDragging, setIsDragging] = useState(false)
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 })
  const [preferCross, setPreferCross] = useState<boolean>(true)
  const [chartErrorsByKpi, setChartErrorsByKpi] = useState<Record<string, any>>({})
  const [lineageOpen, setLineageOpen] = useState(false)
  const [lineageKpi, setLineageKpi] = useState<any>(null)
  const [lineageData, setLineageData] = useState<Lineage | null>(null)
  const [retrievalAssist, setRetrievalAssist] = useState<boolean>(() => {
    try { return JSON.parse(localStorage.getItem('retrievalAssist') || 'false') } catch { return false }
  })
  
  // Add KPI Modal state
  const [addKpiModalOpen, setAddKpiModalOpen] = useState(false)
  const [addKpiDescription, setAddKpiDescription] = useState('')
  const [addKpiClarifyingQuestions, setAddKpiClarifyingQuestions] = useState<string[]>([])
  const [addKpiAnswers, setAddKpiAnswers] = useState<string[]>([])
  const [addKpiLoading, setAddKpiLoading] = useState(false)
  const [addKpiGeneratedKpi, setAddKpiGeneratedKpi] = useState<any>(null)
  const [addKpiStep, setAddKpiStep] = useState<'description' | 'clarifying' | 'generated'>('description')
  const [addKpiEditedSql, setAddKpiEditedSql] = useState('')
  const [addKpiTestResult, setAddKpiTestResult] = useState<any>(null)
  const [addKpiTesting, setAddKpiTesting] = useState(false)

  function applyPalette(p: { primary: string; accent: string; surface: string; warn: string }) {
    const r = document.documentElement
    r.style.setProperty('--primary', p.primary)
    r.style.setProperty('--accent', p.accent)
    r.style.setProperty('--surface', p.surface)
    r.style.setProperty('--warn', p.warn)
  }

  // AI Modal drag handlers
  const handleMouseDown = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      setIsDragging(true)
      const rect = e.currentTarget.getBoundingClientRect()
      setDragOffset({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top
      })
    }
  }

  const handleMouseMove = (e: MouseEvent) => {
    if (isDragging) {
      const modalWidth = 840
      const modalHeight = 70 * window.innerHeight / 100
      
      // Calculate new position
      let newX = e.clientX - dragOffset.x
      let newY = e.clientY - dragOffset.y
      
      // Apply boundary constraints
      newX = Math.max(0, Math.min(newX, window.innerWidth - modalWidth))
      newY = Math.max(0, Math.min(newY, window.innerHeight - modalHeight))
      
      setAiModalPosition({ x: newX, y: newY })
    }
  }

  const handleMouseUp = () => {
    setIsDragging(false)
  }

  useEffect(() => {
    if (isDragging) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
      return () => {
        document.removeEventListener('mousemove', handleMouseMove)
        document.removeEventListener('mouseup', handleMouseUp)
      }
    }
  }, [isDragging, dragOffset])

  useEffect(() => { applyPalette(palette) }, [])

  useEffect(() => {
    setRetrievalAssistEnabled(retrievalAssist)
  }, [retrievalAssist])

  const tabColors = [palette.primary, palette.accent, palette.warn, palette.surface]
  const colorForTab = (id: string, idx: number) => tabColors[idx % tabColors.length]

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  useEffect(() => {
    setLoadError('')
    api.getDatasets().then(datasets => {
      console.log('Loaded datasets:', datasets)
      console.log('Backend-created datasets:', datasets.filter(ds => ds.isBackendCreated))
      console.log('User datasets:', datasets.filter(ds => !ds.isBackendCreated))
      setDatasets(datasets)
    }).catch(() => setLoadError('Failed to fetch datasets. Ensure the Cloud Run service account has BigQuery list permissions.'))
    api.listDashboards().then(dashboards => {
      console.log('Loaded dashboards:', dashboards)
      setDashList(dashboards)
    }).catch(() => {})
  }, [])

  useEffect(() => {
    if (!routeId) return
    console.log('Loading dashboard with routeId:', routeId)
    api.getDashboard(routeId).then(async d => {
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

      // Auto-run KPIs for the active tab on initial load in Editor
      try {
        const active = d.last_active_tab || 'overview'
        const visible = (d.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(active) : active === 'overview'))
        setTimeout(async () => {
          for (const k of visible) {
            try {
              const res = await api.runKpi(k.sql, { date: (d.global_filters && d.global_filters.date) || {} }, k.filter_date_column, k.expected_schema)
              setRowsByKpi(prev => ({ ...prev, [k.id]: res }))
            } catch (e) {
              console.warn('Auto-run KPI failed:', k.name, e)
            }
          }
        }, 0)
      } catch (e) {
        console.warn('Failed during initial KPI auto-run', e)
      }
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
      const kpisResp = await api.generateKpis(selected, 5, preferCross)
      try { sessionStorage.setItem('kpiDrafts', JSON.stringify({ drafts: kpisResp, selectedTables: selected })) } catch {}
      toast('success', `Generated ${kpisResp.length} KPIs. Review and publish from KPI Draft.`)
      navigate('/kpi-draft', { state: { drafts: kpisResp, selectedTables: selected } })
    } catch (error) {
      console.error('Failed to analyze tables:', error)
      toast('error', 'Failed to analyze tables. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  async function runKpi(kpi: any) {
    const filters = {
      date: globalDate,
    }
    try {
      const res = await api.runKpi(kpi.sql, filters, kpi.filter_date_column, kpi.expected_schema)
      setRowsByKpi(prev => ({...prev, [kpi.id]: res}))
    } catch (e: any) {
      // store structured error and auto-open AI Edit
      const errDetail = e?.response?.data?.detail || e?.message || e
      setChartErrorsByKpi(prev => ({ ...prev, [kpi.id]: { runError: errDetail } }))
      setAiEditKpi(kpi)
      setAiChat([{ role: 'assistant', text: 'I saw a run error. I will help fix this KPI. Provide constraints or desired output if any.' }])
      setAiEditOpen(true)
    }
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

  async function saveDashboard() {
    setSaving(true)
    try {
      console.log('Saving dashboard with routeId:', routeId, 'and name:', dashboardName)
      
      const payload = {
        id: routeId, // Always pass the current ID if it exists
        name: dashboardName,
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
      
      console.log('Save payload:', payload)
      const res = await api.saveDashboard(payload as any)
      console.log('Save response:', res)
      
      setVersion(res.version)
      
      // If this is a new dashboard or the ID changed, navigate to the new URL
      if (res.id !== routeId) {
        console.log('Dashboard ID changed from', routeId, 'to', res.id, '- navigating to new URL')
        navigate(`/editor/${res.id}`)
      } else {
        console.log('Dashboard ID unchanged, staying on current route')
      }
      
      await api.listDashboards().then(setDashList)
      toast('success', `Saved ${res.name} v${res.version}`)
      setDirty(false)
    } catch (e: any) {
      console.error('Save dashboard error:', e)
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





  async function addKpiToCanvas(item: any) {
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
    
    // Auto-run the new KPI when it's added
    try {
      toast('success', `Auto-running new KPI: ${k.name}`)
      await runKpi(k)
      toast('success', `KPI "${k.name}" added and executed successfully`)
    } catch (error) {
      console.warn('Failed to auto-run new KPI:', error)
      toast('error', `Failed to auto-run KPI: ${k.name}`)
    }
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

  function openAiEdit(k: any) {
    setAiEditKpi(k)
    setAiChat([{ role: 'assistant', text: 'Let\'s refine this KPI. Tell me what you\'d like to change (chart type, labels, SQL, grouping, filters).' }])
    setAiEditOpen(true)
    
    // Center the modal on screen when it opens
    const centerX = Math.max(0, (window.innerWidth - 840) / 2)
    const centerY = Math.max(0, (window.innerHeight - 70 * window.innerHeight / 100) / 2)
    setAiModalPosition({ x: centerX, y: centerY })
  }

  function openLineage(k: any) {
    try {
      const lin = computeKpiLineage(k.sql, k)
      setLineageKpi(k)
      setLineageData(lin)
      setLineageOpen(true)
    } catch (e) {
      setLineageKpi(k)
      setLineageData({ sources: [], joins: [] })
      setLineageOpen(true)
    }
  }

  function openAddKpiModal() {
    try {
      sessionStorage.setItem('kpiDrafts', JSON.stringify({ drafts: [], selectedTables: selected }))
    } catch {}
    navigate('/kpi-draft', { state: { drafts: [], selectedTables: selected } })
  }

  async function handleAddKpiSubmit() {
    if (!addKpiDescription.trim() || !selected.length) return
    
    setAddKpiLoading(true)
    try {
      const result = await api.generateCustomKpi(selected, addKpiDescription.trim())
      
      if (result.clarifying_questions) {
        setAddKpiClarifyingQuestions(result.clarifying_questions)
        setAddKpiStep('clarifying')
      } else if (result.kpi) {
        setAddKpiGeneratedKpi(result.kpi)
        setAddKpiStep('generated')
      }
    } catch (error) {
      console.error('Failed to generate custom KPI:', error)
      toast('error', 'Failed to generate custom KPI. Please try again.')
    } finally {
      setAddKpiLoading(false)
    }
  }

  async function handleClarifyingQuestionsSubmit() {
    if (addKpiAnswers.length !== addKpiClarifyingQuestions.length) return
    
    setAddKpiLoading(true)
    try {
      const result = await api.generateCustomKpi(selected, addKpiDescription.trim(), addKpiClarifyingQuestions, addKpiAnswers)
      
      if (result.kpi) {
        setAddKpiGeneratedKpi(result.kpi)
        setAddKpiStep('generated')
      }
    } catch (error) {
      console.error('Failed to generate custom KPI:', error)
      toast('error', 'Failed to generate custom KPI. Please try again.')
    } finally {
      setAddKpiLoading(false)
    }
  }

  function handleAddKpiToCanvas() {
    if (!addKpiGeneratedKpi) return
    
    // Add the generated KPI to the canvas, using edited SQL if available
    const newKpi = {
      ...addKpiGeneratedKpi,
      sql: addKpiEditedSql || addKpiGeneratedKpi.sql,
      tabs: [activeTab]
    }
    
    setKpis(prev => [...prev, newKpi])
    
    // Add to layout
    const newLayout = [...(tabLayouts[activeTab] || layouts)]
    const lastKpi = newLayout[newLayout.length - 1] || { x: 0, y: 0, w: 6, h: 8 }
    const newKpiLayout = {
      i: newKpi.id,
      x: (lastKpi.x + lastKpi.w) % 12,
      y: lastKpi.y + lastKpi.h,
      w: 6,
      h: 8
    }
    
    const updatedLayout = [...newLayout, newKpiLayout]
    setLayouts(updatedLayout)
    setTabLayouts(prev => ({ ...prev, [activeTab]: updatedLayout }))
    
    setDirty(true)
    setAddKpiModalOpen(false)
    toast('success', 'Custom KPI added to canvas!')
  }

  async function testAddKpiSql() {
    if (!addKpiGeneratedKpi) return
    
    const sqlToTest = addKpiEditedSql || addKpiGeneratedKpi.sql
    setAddKpiTesting(true)
    
    try {
      const result = await api.runKpi(sqlToTest, { date: globalDate }, addKpiGeneratedKpi.filter_date_column, addKpiGeneratedKpi.expected_schema)
      setAddKpiTestResult(result)
      toast('success', 'SQL test successful!')
    } catch (error) {
      console.error('SQL test failed:', error)
      toast('error', 'SQL test failed. Please check your query.')
      setAddKpiTestResult(null)
    } finally {
      setAddKpiTesting(false)
    }
  }

  async function sendAiEdit() {
    if (!aiEditKpi || !aiInput.trim()) return
    const msg = aiInput.trim()
    setAiChat(prev => [...prev, { role: 'user', text: msg }])
    setAiInput('')
    const history = aiChat.map(m => ({ role: m.role, content: m.text }))
    setAiTyping(true)
    const ctx = { rows: rowsByKpi[aiEditKpi.id] || [], error: chartErrorsByKpi[aiEditKpi.id]?.runError || null, chart_error: chartErrorsByKpi[aiEditKpi.id]?.chartError || null }
    const res = await api.editKpiChat(aiEditKpi, msg, history, ctx)
    if (res.reply) setAiChat(prev => [...prev, { role: 'assistant', text: res.reply }])
    if (res.kpi) {
      const idx = kpis.findIndex(x => x.id === aiEditKpi.id)
      if (idx >= 0) {
        const next = [...kpis]
        next[idx] = { ...next[idx], ...res.kpi }
        setKpis(next)
        setDirty(true)
        setAiEditKpi(next[idx])
      }
    }
    setAiTyping(false)
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
          onSave={() => saveDashboard()}
          globalDate={globalDate}
          onGlobalDateChange={(v) => { setGlobalDate(v); setDirty(true) }}
          theme={theme}
          onThemeToggle={() => setTheme(t => (t === 'light' ? 'dark' : 'light'))}
          onExportDashboard={exportDashboard}
          onToggleSidebar={() => setSidebarOpen(o => !o)}
          sidebarOpen={sidebarOpen}
          dirty={dirty}
          dashboardId={routeId}
        />

      <div className={`app-grid ${!sidebarOpen ? 'app-grid--collapsed' : ''}`}>
        {(
          <div className={`sidebar ${!sidebarOpen ? 'is-collapsed' : ''}`} style={{ display: 'grid', gap: 12 }}>
            {/* Retrieval Assist toggle at top */}
            <div className="panel" style={{ padding: 10 }}>
              <label className="card-subtitle" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <input type="checkbox" checked={retrievalAssist} onChange={e => setRetrievalAssist(e.target.checked)} />
                Retrieval Assist (use prior edits)
              </label>
            </div>
            {/* left panels */}
            <div className="panel">
              <div className="section-title">Data</div>
              {loadError && <div className="badge" style={{ marginBottom: 8, borderColor: 'crimson', color: 'crimson', background: 'rgba(220,20,60,0.06)' }}>{loadError}</div>}
              {datasets.length === 0 ? (
                <div className="card-subtitle">No datasets found. Verify IAM and BigQuery project.</div>
              ) : (
                <>
                  <div style={{ fontSize: '12px', color: '#666', marginBottom: 8, fontStyle: 'italic' }}>
                    {datasets.filter(ds => !ds.isBackendCreated).length} of {datasets.length} datasets available
                  </div>
                  <TableSelector datasets={datasets} onChange={setSelected} />
                </>
              )}
              <div style={{ display: 'flex', gap: 8, marginTop: 8, alignItems: 'center' }}>
                <button className="btn btn-primary" onClick={onAnalyze} disabled={!selected.length || loading}>
                  {loading ? 'Analyzing...' : `Analyze (${selected.length})`}
                </button>
                <button className="btn btn-secondary" onClick={openAddKpiModal} disabled={!selected.length}>
                  Add KPI
                </button>
                <label className="card-subtitle" style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
                  <input type="checkbox" checked={preferCross} onChange={e => setPreferCross(e.target.checked)} /> Prefer cross-table KPIs
                </label>
              </div>
            </div>
            <div className="panel">
              <div className="section-title">KPI Catalog</div>
              <KPICatalog key={catalogRefreshKey} onAdd={addKpiToCanvas} />
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

                    // Auto-run KPIs on dashboard switch in Editor
                    try {
                      const active = d.last_active_tab || 'overview'
                      const visible = (d.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(active) : active === 'overview'))
                      setTimeout(async () => {
                        for (const k of visible) {
                          try {
                            const res = await api.runKpi(k.sql, { date: (d.global_filters && d.global_filters.date) || {} }, k.filter_date_column, k.expected_schema)
                            setRowsByKpi(prev => ({ ...prev, [k.id]: res }))
                          } catch (e) {
                            console.warn('Auto-run KPI failed:', k.name, e)
                          }
                        }
                      }, 0)
                    } catch (e) {
                      console.warn('Failed during KPI auto-run on dashboard switch', e)
                    }
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
            
            {/* Theme section moved to bottom */}
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
          </div>
        )}

        <div style={{ display: 'grid', gap: 12 }} ref={gridWrapRef}>
          <div className="section-title">
            Dashboard {version && <span className="chip" style={{ marginLeft: 8 }}>v{version}</span>}
          </div>

          <div className="tabs-bar">
            <button className="tab-arrow" onClick={() => { const el = document.getElementById('tabs-scroll'); if (el) el.scrollBy({ left: -160, behavior: 'smooth' }) }}>❮</button>
            <div id="tabs-scroll" className="tabs-scroll">
              {tabs.sort((a,b)=>a.order-b.order).map((t, idx) => (
                <div
                  key={t.id}
                  className={`tab-pill ${t.id===activeTab ? 'active' : ''}`}
                  style={{ borderColor: t.id===activeTab? colorForTab(t.id, idx): undefined, background: t.id===activeTab? colorForTab(t.id, idx): undefined, color: t.id===activeTab? '#fff': undefined }}
                  onClick={() => setActiveTab(t.id)}
                  draggable
                  onDragStart={(e) => onDragStartTab(t.id, e)}
                  onDragOver={onDragOverTab}
                  onDrop={(e) => onDropTab(t.id, e)}
                  onDoubleClick={() => startEditTab(t)}
                  title="Drag to reorder. Double-click to rename."
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
                      style={{ maxWidth: 160 }}
                    />
                  ) : (
                    <span>{t.name}</span>
                  )}
                </div>
              ))}
            </div>
            <button className="tab-arrow" onClick={() => { const el = document.getElementById('tabs-scroll'); if (el) el.scrollBy({ left: 160, behavior: 'smooth' }) }}>❯</button>
            <button className="btn btn-sm" onClick={addTab}>+ Tab</button>
            {activeTab !== 'overview' && <button className="btn btn-sm" onClick={() => { removeTab(activeTab); setDirty(true) }}>Delete Tab</button>}
          </div>
          <GridLayout className="layout" layout={activeLayout} cols={12} rowHeight={30} width={gridW} isResizable isDraggable draggableHandle=".drag-handle" draggableCancel=".no-drag, button, input, textarea, select" onLayoutChange={onLayoutChange}>
            {visibleKpis.map(k => (
              <div key={k.id} data-grid={(activeLayout||[]).find(l => l.i === k.id)} className="card" style={{ display: 'flex', flexDirection: 'column' }}>
                <div className="card-header">
                  <div className="drag-handle">
                    <div className="card-title">{k.name}</div>
                    <div className="card-subtitle">{k.short_description}</div>
                    {!rowsByKpi[k.id] && <span className="badge" style={{ marginLeft: 8, background: 'var(--accent)', color: '#fff', fontSize: '10px' }}>Loading...</span>}
                  </div>
                  <div className="card-actions no-drag">
                    <button className="btn btn-sm" onClick={() => runKpi(k)}>Test</button>
                    <button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View SQL</button>
                    <button className="btn btn-sm" onClick={() => openAiEdit(k)}>AI Edit</button>
                    <button className="btn btn-sm" onClick={() => openLineage(k)}>KPI Lineage</button>
                    <button className="btn btn-sm" onClick={async () => {
                      const r = await fetch('/api/export/card', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql: k.sql }) }); const blob = await r.blob(); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `${k.name||'card'}.csv`; a.click(); URL.revokeObjectURL(url)
                    }}>Export</button>
                    <button className="btn btn-sm" onClick={() => { const nextKpis = kpis.filter(x => x.id !== k.id); const nextLayout = (activeLayout||[]).filter(l => l.i !== k.id); setKpis(nextKpis); setLayouts(nextLayout); setTabLayouts(prev => ({ ...prev, [activeTab]: nextLayout })); setDirty(true) }}>Remove</button>
                  </div>
                </div>
                <div style={{ flex: 1, padding: 8 }} className="no-drag"><ChartRenderer chart={k} rows={rowsByKpi[k.id] || []} onSelect={(p) => setCrossFilter(p)} onError={(err) => setChartErrorsByKpi(prev => ({ ...prev, [k.id]: { ...(prev[k.id]||{}), chartError: err } }))} /></div>
              </div>
            ))}
          </GridLayout>
        </div>
      </div>
      {aiEditOpen && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9999 }}>
          <div 
            style={{ 
              position: 'absolute',
              left: aiModalPosition.x,
              top: aiModalPosition.y,
              width: 'min(840px, 92vw)', 
              height: 'min(70vh, 85vh)', 
              background: 'var(--card)', 
              border: '1px solid var(--border)', 
              borderRadius: 12, 
              display: 'flex', 
              flexDirection: 'column',
              cursor: isDragging ? 'grabbing' : 'default',
              transition: isDragging ? 'none' : 'box-shadow 0.2s ease',
              boxShadow: isDragging ? '0 8px 32px rgba(0,0,0,0.3)' : 'var(--shadow)'
            }}
          >
            <div 
              style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'space-between', 
                padding: 12, 
                borderBottom: '1px solid var(--border)',
                cursor: 'grab',
                userSelect: 'none'
              }}
              onMouseDown={handleMouseDown}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '14px', opacity: 0.6 }}>⋮⋮</span>
                <div className="card-title">AI Edit: {aiEditKpi?.name}</div>
              </div>
              <div className="toolbar">
                <button className="btn btn-sm" onClick={() => setAiEditOpen(false)}>✕</button>
              </div>
            </div>
            <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 0, minHeight: 0 }}>
              <div style={{ borderRight: '1px solid var(--border)', padding: 12, overflow: 'auto' }}>
                {aiChat.map((m, i) => (
                  <div key={i} style={{ marginBottom: 12 }}>
                    <div className="card-subtitle" style={{ marginBottom: 4 }}>{m.role === 'user' ? 'You' : 'Assistant'}</div>
                    {m.role === 'assistant' ? <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.text}</ReactMarkdown> : <div>{m.text}</div>}
                  </div>
                ))}
                {aiTyping && (
                  <div style={{ marginBottom: 12 }}>
                    <div className="typing"><span className="dot"></span><span className="dot"></span><span className="dot"></span></div>
                  </div>
                )}
              </div>
              <div style={{ padding: 12, overflow: 'auto' }}>
                <div className="card-subtitle" style={{ marginBottom: 8 }}>Current KPI</div>
                <pre style={{ whiteSpace: 'pre-wrap' }}><code>{aiEditKpi?.sql}</code></pre>
                {aiEditKpi?.vega_lite_spec && <div className="card-subtitle" style={{ marginTop: 8 }}>Vega-Lite spec present</div>}
              </div>
            </div>
            <div style={{ padding: 10, borderTop: '1px solid var(--border)', display: 'flex', gap: 8, position: 'sticky', bottom: 0, background: 'var(--card)' }}>
              <input className="input" placeholder="Describe the change..." value={aiInput} onChange={e => setAiInput(e.target.value)} onKeyDown={e => { if (e.key==='Enter') sendAiEdit() }} style={{ flex: 1 }} />
              <button className="btn btn-primary" onClick={sendAiEdit}>Send</button>
            </div>
          </div>
        </div>
      )}
      {lineageOpen && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9999 }} onClick={() => setLineageOpen(false)}>
          <div 
            style={{ 
              position: 'absolute',
              left: '50%',
              top: '50%',
              transform: 'translate(-50%, -50%)',
              width: 'min(820px, 92vw)', 
              height: 'min(70vh, 85vh)', 
              background: 'var(--card)', 
              border: '1px solid var(--border)', 
              borderRadius: 12, 
              display: 'flex', 
              flexDirection: 'column',
              boxShadow: 'var(--shadow)'
            }}
            onClick={e => e.stopPropagation()}
          >
            <div 
              style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'space-between', 
                padding: 12, 
                borderBottom: '1px solid var(--border)'
              }}
            >
              <div style={{ display: 'flex', gap: 8, alignItems: 'baseline' }}>
                <div className="card-title">KPI Lineage</div>
                {lineageKpi?.name && <div className="card-subtitle">{lineageKpi.name}</div>}
              </div>
              <div className="toolbar">
                <button className="btn btn-sm" onClick={() => setLineageOpen(false)}>✕</button>
              </div>
            </div>
            <div style={{ padding: 12, overflow: 'auto', display: 'grid', gap: 12 }}>
              <div className="panel">
                <div className="section-title">Overview</div>
                <div className="card-subtitle">ID: {lineageKpi?.id}</div>
                <div className="card-subtitle">Schema: {lineageKpi?.expected_schema}</div>
                <div className="card-subtitle">Chart: {lineageKpi?.chart_type}</div>
                {lineageKpi?.filter_date_column && <div className="card-subtitle">Filter Date Column: {lineageKpi.filter_date_column}</div>}
              </div>
              <div className="panel">
                <div className="section-title">Sources</div>
                <div className="scroll">
                  {(lineageData?.sources || []).length ? (
                    (lineageData?.sources || []).map(s => (
                      <div key={s} className="list-item"><span style={{ flex: 1 }}>{s}</span></div>
                    ))
                  ) : (
                    <div className="card-subtitle">No sources detected.</div>
                  )}
                </div>
              </div>
              <div className="panel">
                <div className="section-title">Joins</div>
                <div className="scroll">
                  {(lineageData?.joins || []).length ? (
                    (lineageData?.joins || []).map((j, idx) => (
                      <div key={idx} className="list-item">
                        <span style={{ flex: 1 }}>{j.left} = {j.right}</span>
                        <span className="card-subtitle" style={{ fontSize: 11, opacity: 0.8 }}>{j.on}</span>
                      </div>
                    ))
                  ) : (
                    <div className="card-subtitle">No joins detected.</div>
                  )}
                </div>
              </div>
              <div className="panel">
                <div className="section-title">Filters</div>
                {(lineageData?.filters || []).length ? (
                  (lineageData?.filters || []).map((f, idx) => (
                    <div key={idx} className="list-item"><span style={{ flex: 1 }}>{f}</span></div>
                  ))
                ) : (
                  <div className="card-subtitle">No filters detected.</div>
                )}
              </div>
              <div className="panel">
                <div className="section-title">Group By / Outputs</div>
                {lineageData?.groupBy && lineageData.groupBy.length > 0 && (
                  <div className="card-subtitle">Group By: {lineageData.groupBy.join(', ')}</div>
                )}
                {lineageData?.outputs && (
                  <div className="card-subtitle">Outputs: {Object.entries(lineageData.outputs).filter(([,v]) => Boolean(v)).map(([k,v]) => `${k}: ${(v as string).replace(/\s+/g,' ')}`).join(' | ')}</div>
                )}
              </div>
              <div className="panel">
                <div className="section-title">Raw JSON</div>
                <div className="toolbar" style={{ marginBottom: 8 }}>
                  <button className="btn btn-sm" onClick={() => { try { navigator.clipboard.writeText(JSON.stringify(lineageData, null, 2)) } catch {} }}>Copy JSON</button>
                </div>
                <pre style={{ maxHeight: 220, overflow: 'auto', fontSize: 11 }}><code>{JSON.stringify(lineageData, null, 2)}</code></pre>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {/* Add KPI Modal */}
      {addKpiModalOpen && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9999 }}>
          <div 
            style={{ 
              position: 'absolute',
              left: '50%',
              top: '50%',
              transform: 'translate(-50%, -50%)',
              width: 'min(600px, 90vw)', 
              height: 'min(70vh, 85vh)', 
              background: 'var(--card)', 
              border: '1px solid var(--border)', 
              borderRadius: 12, 
              display: 'flex', 
              flexDirection: 'column',
              boxShadow: 'var(--shadow)'
            }}
          >
            <div 
              style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'space-between', 
                padding: 16, 
                borderBottom: '1px solid var(--border)'
              }}
            >
              <div className="card-title">Add Custom KPI</div>
              <button className="btn btn-sm" onClick={() => setAddKpiModalOpen(false)}>✕</button>
            </div>
            
            <div style={{ flex: 1, padding: 16, overflow: 'auto' }}>
              {addKpiStep === 'description' && (
                <div>
                  <div className="card-subtitle" style={{ marginBottom: 16 }}>
                    Describe the KPI you want to create. Be specific about what metrics, dimensions, and chart type you need.
                  </div>
                  <textarea 
                    className="input" 
                    placeholder="e.g., Show me a line chart of daily revenue over time, grouped by product category"
                    value={addKpiDescription}
                    onChange={e => setAddKpiDescription(e.target.value)}
                    style={{ width: '100%', minHeight: 100, resize: 'vertical' }}
                  />
                  <div style={{ marginTop: 16 }}>
                    <button 
                      className="btn btn-primary" 
                      onClick={handleAddKpiSubmit}
                      disabled={!addKpiDescription.trim() || addKpiLoading}
                    >
                      {addKpiLoading ? 'Generating...' : 'Generate KPI'}
                    </button>
                  </div>
                </div>
              )}
              
              {addKpiStep === 'clarifying' && (
                <div>
                  <div className="card-subtitle" style={{ marginBottom: 16 }}>
                    Please answer these questions to help generate your KPI:
                  </div>
                  {addKpiClarifyingQuestions.map((question, index) => (
                    <div key={index} style={{ marginBottom: 16 }}>
                      <div className="card-subtitle" style={{ marginBottom: 8 }}>{question}</div>
                      <input 
                        className="input" 
                        placeholder="Your answer..."
                        value={addKpiAnswers[index] || ''}
                        onChange={e => {
                          const newAnswers = [...addKpiAnswers]
                          newAnswers[index] = e.target.value
                          setAddKpiAnswers(newAnswers)
                        }}
                      />
                    </div>
                  ))}
                  <div style={{ marginTop: 16 }}>
                    <button 
                      className="btn btn-primary" 
                      onClick={handleClarifyingQuestionsSubmit}
                      disabled={addKpiAnswers.length !== addKpiClarifyingQuestions.length || addKpiLoading}
                    >
                      {addKpiLoading ? 'Generating...' : 'Generate KPI'}
                    </button>
                  </div>
                </div>
              )}
              
              {addKpiStep === 'generated' && addKpiGeneratedKpi && (
                <div>
                  <div className="card-subtitle" style={{ marginBottom: 16 }}>
                    Your custom KPI has been generated! Review the details below:
                  </div>
                  
                  <div style={{ marginBottom: 16 }}>
                    <div className="card-subtitle" style={{ marginBottom: 8 }}>Name</div>
                    <div>{addKpiGeneratedKpi.name}</div>
                  </div>
                  
                  <div style={{ marginBottom: 16 }}>
                    <div className="card-subtitle" style={{ marginBottom: 8 }}>Description</div>
                    <div>{addKpiGeneratedKpi.short_description}</div>
                  </div>
                  
                  <div style={{ marginBottom: 16 }}>
                    <div className="card-subtitle" style={{ marginBottom: 8 }}>Chart Type</div>
                    <div>{addKpiGeneratedKpi.chart_type}</div>
                  </div>
                  
                  <div style={{ marginBottom: 16 }}>
                    <div className="card-subtitle" style={{ marginBottom: 8 }}>SQL Query</div>
                    <div style={{ marginBottom: 8 }}>
                      <textarea 
                        className="input" 
                        value={addKpiEditedSql || addKpiGeneratedKpi.sql}
                        onChange={e => setAddKpiEditedSql(e.target.value)}
                        style={{ width: '100%', minHeight: 120, resize: 'vertical', fontFamily: 'monospace', fontSize: '12px' }}
                        placeholder="Edit SQL query here..."
                      />
                    </div>
                    <div style={{ fontSize: '12px', color: '#666', fontStyle: 'italic' }}>
                      You can edit the SQL query above before adding it to the canvas.
                    </div>
                    
                    {addKpiTestResult && (
                      <div style={{ marginTop: 16, padding: 12, background: 'var(--surface)', borderRadius: 8, border: '1px solid var(--border)' }}>
                        <div className="card-subtitle" style={{ marginBottom: 8 }}>Test Results</div>
                        <div style={{ fontSize: '12px', color: '#666' }}>
                          Query returned {addKpiTestResult.length} rows
                        </div>
                        {addKpiTestResult.length > 0 && (
                          <div style={{ marginTop: 8 }}>
                            <div className="card-subtitle" style={{ marginBottom: 4, fontSize: '11px' }}>Sample Data:</div>
                            <pre style={{ fontSize: '10px', overflow: 'auto', maxHeight: '100px' }}>
                              {JSON.stringify(addKpiTestResult.slice(0, 3), null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                  
                  <div style={{ marginBottom: 16 }}>
                    <div className="card-subtitle" style={{ marginBottom: 8 }}>Actions</div>
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      <button 
                        className="btn btn-sm" 
                        onClick={() => window.alert(addKpiGeneratedKpi.sql)}
                        title="View SQL in alert"
                      >
                        View SQL
                      </button>
                      <button 
                        className="btn btn-sm" 
                        onClick={() => {
                          const newWindow = window.open('', '_blank')
                          if (newWindow) {
                            newWindow.document.write(`
                              <html>
                                <head><title>SQL Query</title></head>
                                <body>
                                  <pre style="font-family: monospace; padding: 20px;">${addKpiGeneratedKpi.sql}</pre>
                                </body>
                              </html>
                            `)
                          }
                        }}
                        title="Open SQL in new window"
                      >
                        Open SQL
                      </button>
                      <button 
                        className="btn btn-sm" 
                        onClick={testAddKpiSql}
                        disabled={addKpiTesting}
                        title="Test the SQL query"
                      >
                        {addKpiTesting ? 'Testing...' : 'Test SQL'}
                      </button>
                    </div>
                  </div>
                  
                  <div style={{ marginTop: 16, display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary" onClick={handleAddKpiToCanvas}>
                      Add to Canvas
                    </button>
                    <button className="btn btn-secondary" onClick={() => setAddKpiStep('description')}>
                      Start Over
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}