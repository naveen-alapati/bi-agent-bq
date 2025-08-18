import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../services/api'
import GridLayout, { Layout } from 'react-grid-layout'
import { ChartRenderer } from '../ui/ChartRenderer'
import '../styles.css'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import html2canvas from 'html2canvas'
import jsPDF from 'jspdf'

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
  const [cxoOpen, setCxoOpen] = useState<boolean>(false)
  const [cxoMin, setCxoMin] = useState<boolean>(false)
  const [convId, setConvId] = useState<string>('')
  const [chat, setChat] = useState<{ role: 'assistant'|'user'; text: string }[]>([])
  const [input, setInput] = useState('')
  const chatWrapRef = useRef<HTMLDivElement | null>(null)
  const [chatPos, setChatPos] = useState<{ x: number; y: number }>({ x: 16, y: 16 })
  const [chatSize, setChatSize] = useState<{ w: number; h: number }>({ w: 520, h: 0 })
  const feedLayerRef = useRef<HTMLDivElement | null>(null)
  const [exportOpen, setExportOpen] = useState(false)

  useEffect(() => { api.listDashboards().then((rows) => {
    // dedupe by name, keep latest updated_at
    const byName: Record<string, any> = {}
    for (const d of rows || []) {
      const key = d.name
      const prev = byName[key]
      if (!prev) byName[key] = d
      else {
        const prevTs = Date.parse(prev.updated_at || prev.created_at || '0')
        const curTs = Date.parse(d.updated_at || d.created_at || '0')
        if (curTs >= prevTs) byName[key] = d
      }
    }
    setDashboards(Object.values(byName))
  }).catch(() => {}) }, [])
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

  function animateBezier(node: HTMLElement, sx: number, sy: number, dx: number, dy: number, durationMs: number, delayMs: number, curveOffset = 120) {
    const cx = (sx + dx) / 2 + (Math.random() * 2 - 1) * curveOffset
    const cy = (sy + dy) / 2 - (Math.random() * 2 - 1) * (curveOffset * 0.6)
    const start = performance.now() + delayMs
    function step(now: number) {
      if (now < start) { requestAnimationFrame(step); return }
      const t = Math.min(1, (now - start) / durationMs)
      const omt = 1 - t
      const x = omt * omt * sx + 2 * omt * t * cx + t * t * dx
      const y = omt * omt * sy + 2 * omt * t * cy + t * t * dy
      const scale = 0.95 - 0.35 * t
      const rot = (t * 20) * (Math.random() > 0.5 ? 1 : -1)
      node.style.transform = `translate(${x - sx}px, ${y - sy}px) scale(${scale}) rotate(${rot}deg)`
      node.style.opacity = `${1 - t}`
      if (t < 1) requestAnimationFrame(step)
      else try { node.remove() } catch {}
    }
    requestAnimationFrame(step)
  }

  // Snapshot and animate mini charts into chat with curved paths and icon overlays
  async function animateMiniCharts() {
    try {
      const overlay = feedLayerRef.current
      const chatEl = chatWrapRef.current
      if (!overlay || !chatEl) return
      const chatRect = chatEl.getBoundingClientRect()
      const destX = chatRect.left + chatRect.width - 80
      const destY = chatRect.top + 60
      const cards = Array.from(document.querySelectorAll('.layout .card')) as HTMLElement[]
      const take = cards.slice(0, 6)
      for (let idx = 0; idx < take.length; idx++) {
        const card = take[idx]
        const r = card.getBoundingClientRect()
        const sx = r.left + 12
        const sy = r.top + 12
        const target = card.querySelector('.no-drag') as HTMLElement
        if (!target) continue
        const canvas = await html2canvas(target, { backgroundColor: null, scale: 0.28 })
        const wrap = document.createElement('div')
        wrap.style.position = 'fixed'
        wrap.style.left = `${sx}px`
        wrap.style.top = `${sy}px`
        wrap.style.willChange = 'transform, opacity'
        wrap.style.opacity = '0.98'
        const img = document.createElement('img')
        img.src = canvas.toDataURL('image/png')
        img.style.width = `${Math.max(140, Math.min(220, r.width * 0.32))}px`
        img.style.height = 'auto'
        img.style.borderRadius = '10px'
        img.style.boxShadow = '0 10px 28px rgba(0,0,0,0.22)'
        // icon overlay
        const icon = document.createElement('div')
        icon.textContent = '📊'
        icon.style.position = 'absolute'
        icon.style.right = '-8px'
        icon.style.top = '-8px'
        icon.style.width = '24px'
        icon.style.height = '24px'
        icon.style.borderRadius = '50%'
        icon.style.display = 'flex'
        icon.style.alignItems = 'center'
        icon.style.justifyContent = 'center'
        icon.style.background = 'var(--primary)'
        icon.style.color = '#fff'
        icon.style.fontSize = '14px'
        icon.style.boxShadow = '0 2px 8px rgba(0,0,0,0.18)'
        wrap.appendChild(img)
        wrap.appendChild(icon)
        overlay.appendChild(wrap)
        // animate along bezier
        const delay = 80 + idx * 160
        animateBezier(wrap, sx, sy, destX, destY, 1200, delay, 120 + Math.random() * 80)
      }
    } catch {}
  }

  function onDragChat(e: React.MouseEvent) {
    const startX = e.clientX, startY = e.clientY
    const origin = { ...chatPos }
    function move(ev: MouseEvent) {
      setChatPos({ x: Math.max(8, origin.x + (ev.clientX - startX)), y: Math.max(8, origin.y + (ev.clientY - startY)) })
    }
    function up() { window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up) }
    window.addEventListener('mousemove', move)
    window.addEventListener('mouseup', up)
  }

  async function openCxo() {
    if (!active) return
    if (!convId) {
      const id = await api.cxoStart(active.id, active.name, activeTab)
      setConvId(id)
      const welcome = `Hello Naveen Alapati. I am your CXO AI Assist. How can I help today?`
      setChat([{ role: 'assistant', text: welcome }])
    }
    setCxoOpen(true); setCxoMin(false)
    setTimeout(() => animateMiniCharts(), 220)
  }

  const [showFeed, setShowFeed] = useState(false)

  async function quickSummary() {
    if (!active) return
    const context = {
      dashboard_name: active.name,
      active_tab: activeTab,
      kpis: (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: rowsByKpi[k.id] || [] }))
    }
    const id = convId || await api.cxoStart(active.id, active.name, activeTab)
    if (!convId) setConvId(id)
    const msg = 'Generate executive summary from available data.'
    setChat(prev => [...prev, { role: 'user', text: msg }])
    const reply = await api.cxoSend(id, msg, context)
    setChat(prev => [...prev, { role: 'assistant', text: reply }])
  }

  async function sendCxo() {
    if (!convId || !input.trim() || !active) return
    const msg = input.trim()
    setChat(prev => [...prev, { role: 'user', text: msg }])
    setInput('')
    const context = {
      dashboard_name: active.name,
      active_tab: activeTab,
      kpis: (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: rowsByKpi[k.id] || [] }))
    }
    const reply = await api.cxoSend(convId, msg, context)
    setChat(prev => [...prev, { role: 'assistant', text: reply }])
  }

  async function captureChartImage(card: HTMLElement): Promise<HTMLCanvasElement | null> {
    const target = card.querySelector('.no-drag') as HTMLElement
    if (!target) return null
    try {
      return await html2canvas(target, { backgroundColor: '#ffffff', scale: 2 })
    } catch { return null }
  }

  function addSummaryPage(doc: jsPDF, title: string, mdText: string) {
    const margin = 14
    doc.setFont('helvetica', 'bold')
    doc.setFontSize(18)
    doc.text('CXO Summary', margin, 20)
    doc.setFont('helvetica', 'normal')
    doc.setFontSize(11)
    const dateStr = new Date().toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
    doc.text(dateStr, margin, 28)
    doc.setFontSize(12)
    const lines = doc.splitTextToSize(mdText.replace(/\r\n/g, '\n'), 180)
    doc.text(lines as any, margin, 40)
    doc.setDrawColor(230)
    doc.line(margin, 36, 210 - margin, 36)
    doc.setTextColor(0,0,238)
    doc.textWithLink('Open Dashboard', 210 - margin - 42, 28, { url: 'https://analytics-kpi-poc-315425729064.asia-south1.run.app' })
    doc.setTextColor(0,0,0)
  }

  async function exportCurrentDashboardPDF() {
    if (!active) return
    const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' })
    // Generate latest summary
    const context = {
      dashboard_name: active.name,
      active_tab: activeTab,
      kpis: (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: rowsByKpi[k.id] || [] }))
    }
    const id = convId || await api.cxoStart(active.id, active.name, activeTab)
    const summary = await api.cxoSend(id, 'Generate executive summary from available data.', context)
    addSummaryPage(doc, 'CXO Summary', summary)
    // Charts: 2 per page vertically
    const cards = Array.from(document.querySelectorAll('.layout .card')) as HTMLElement[]
    const visible = (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview'))
    let slot = 0
    for (const k of visible) {
      const card = cards.find(c => (c.getAttribute('data-grid') || '').includes(`"i":"${k.id}"`)) || cards.find(c => c.innerText.includes(k.name))
      if (!card) continue
      const canvas = await captureChartImage(card)
      if (!canvas) continue
      if (slot > 0) doc.addPage()
      const imgW = 180, imgH = (canvas.height / canvas.width) * imgW
      doc.addImage(canvas.toDataURL('image/png'), 'PNG', 15, 18, imgW, Math.min(imgH, 120))
      // second slot on same page if available
      const idx = visible.indexOf(k)
      const next = visible[idx + 1]
      if (next) {
        const card2 = cards.find(c => (c.getAttribute('data-grid') || '').includes(`"i":"${next.id}"`)) || cards.find(c => c.innerText.includes(next.name))
        if (card2) {
          const canvas2 = await captureChartImage(card2)
          if (canvas2) {
            doc.addPage()
            const imgH2 = (canvas2.height / canvas2.width) * imgW
            doc.addImage(canvas2.toDataURL('image/png'), 'PNG', 15, 18, imgW, Math.min(imgH2, 120))
          }
        }
      }
      slot += 2
    }
    doc.save(`${active.name || 'dashboard'}-cxo-summary.pdf`)
  }

  async function exportAllDashboardsPDF() {
    const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' })
    const list = dashboards
    let first = true
    for (const d of list) {
      // load dashboard for context
      const full = await api.getDashboard(d.id)
      const tabsFor = (full.tabs && full.tabs.length ? full.tabs : [{ id: 'overview', name: 'Overview' }])
      const activeTabId = (full.last_active_tab || 'overview')
      // generate summary for that dashboard
      const context = {
        dashboard_name: full.name,
        active_tab: activeTabId,
        kpis: (full.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTabId) : activeTabId === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: [] }))
      }
      const id = await api.cxoStart(full.id, full.name, activeTabId)
      const summary = await api.cxoSend(id, 'Generate executive summary from available data.', context)
      if (!first) doc.addPage()
      addSummaryPage(doc, 'CXO Summary', `# ${full.name}\n\n${summary}`)
      first = false
      // Note: cross-context chart capture for all dashboards is complex in one session; for v1 we include summaries only for consolidated export
    }
    doc.save(`all-dashboards-cxo-summary.pdf`)
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
          <button className="btn btn-accent" onClick={openCxo}>CXO AI Assist</button>
          <div style={{ position: 'relative' }}>
            <button className="btn" onClick={() => setExportOpen(o => !o)}>Export CXO Summary ▾</button>
            {exportOpen && (
              <div style={{ position: 'absolute', right: 0, top: '110%', background: 'var(--card)', border: '1px solid var(--border)', boxShadow: 'var(--shadow)', borderRadius: 8, zIndex: 20 }}>
                <button className="btn" onClick={() => { setExportOpen(false); exportCurrentDashboardPDF() }} style={{ display: 'block', width: '100%' }}>Current Dashboard (PDF)</button>
                <button className="btn" onClick={() => { setExportOpen(false); exportAllDashboardsPDF() }} style={{ display: 'block', width: '100%' }}>All Dashboards (PDF)</button>
              </div>
            )}
          </div>
          <a className="btn" href="/editor">New Dashboard</a>
          {active && <a className="btn btn-primary" href={`/editor/${active.id}`}>Edit Dashboard</a>}
        </div>
      </div>

      {/* Floating Chat (side dock, draggable) */}
      {cxoOpen && (
        <div ref={chatWrapRef} style={{ position: 'fixed', right: chatPos.x, bottom: chatPos.y, width: cxoMin ? 340 : 600, height: cxoMin ? 64 : '70vh', background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 12, boxShadow: 'var(--shadow)', display: 'flex', flexDirection: 'column', zIndex: 9998 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: 10, borderBottom: '1px solid var(--border)', cursor: 'move' }} onMouseDown={onDragChat}>
            <div className="card-title">CXO AI Assist</div>
            <div className="toolbar">
              <button className="btn btn-sm" onClick={quickSummary}>Generate CXO Summary</button>
              <button className="btn btn-sm" onClick={() => setCxoMin(m => !m)}>{cxoMin ? '▣' : '–'}</button>
              <button className="btn btn-sm" onClick={() => setCxoOpen(false)}>✕</button>
            </div>
          </div>
          {!cxoMin && (
            <>
              <div style={{ position: 'relative', flex: 1, overflow: 'auto', padding: 12 }}>
                <div ref={feedLayerRef} style={{ position: 'fixed', inset: 0, pointerEvents: 'none', zIndex: 9997 }} />
                {chat.map((m, i) => (
                  <div key={i} style={{ marginBottom: 12, textAlign: m.role==='user' ? 'right':'left' }}>
                    <div style={{ display: 'inline-block', padding: '12px 14px', borderRadius: 12, background: m.role==='user' ? 'var(--primary)' : 'var(--surface)', color: m.role==='user' ? '#fff' : 'var(--fg)', maxWidth: 680, textAlign: 'left' }}>
                      {m.role==='assistant' ? (
                        <ReactMarkdown remarkPlugins={[remarkGfm]} components={{
                          code({node, inline, className, children, ...props}) {
                            const match = /language-(\w+)/.exec(className || '')
                            return !inline && match ? (
                              <SyntaxHighlighter style={oneDark} language={match[1]} PreTag="div" {...props}>
                                {String(children).replace(/\n$/, '')}
                              </SyntaxHighlighter>
                            ) : (
                              <code className={className} {...props}>{children}</code>
                            )
                          }
                        }}>
                          {m.text}
                        </ReactMarkdown>
                      ) : m.text}
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ padding: 10, borderTop: '1px solid var(--border)', display: 'flex', gap: 8 }}>
                <input className="input" placeholder="Ask anything..." value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => { if (e.key==='Enter') sendCxo() }} style={{ flex: 1 }} />
                <button className="btn btn-primary" onClick={sendCxo}>Send</button>
              </div>
            </>
          )}
        </div>
      )}

      <div className={`app-grid ${!sidebarOpen ? 'app-grid--collapsed' : ''}`}>
        {(
          <div className={`sidebar ${!sidebarOpen ? 'is-collapsed' : ''}`} style={{ display: 'grid', gap: 12 }}>
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
                {/* local filters */}
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
                <div className="toolbar" style={{ marginLeft: 'auto' }}>
                  <button className="btn" onClick={() => refreshAll()}>Refresh</button>
                  <button className="btn" onClick={async () => {
                    const kpis = visibleKpis
                    const r = await fetch('/api/export/dashboard', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ kpis }) });
                    const blob = await r.blob(); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `${active.name || 'dashboard'}-${activeTab}.zip`; a.click(); URL.revokeObjectURL(url)
                  }}>Export Current Tab</button>
                  <button className="btn" onClick={async () => {
                    const kpis = active.kpis || []
                    const r = await fetch('/api/export/dashboard', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ kpis }) });
                    const blob = await r.blob(); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `${active.name || 'dashboard'}.zip`; a.click(); URL.revokeObjectURL(url)
                  }}>Export All</button>
                </div>
              </div>

              <div className="toolbar" style={{ gap: 6 }}>
                {tabs.sort((a,b)=>a.order-b.order).map(t => (
                  <button key={t.id} className="btn btn-sm" style={{ background: t.id===activeTab? 'var(--primary)':'', color: t.id===activeTab? '#fff': undefined, borderColor: t.id===activeTab? 'var(--primary)':'' }} onClick={() => setActiveTab(t.id)}>{t.name}</button>
                ))}
              </div>

              <GridLayout className="layout" layout={layout} cols={12} rowHeight={30} width={gridW} isResizable={false} isDraggable={false}>
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