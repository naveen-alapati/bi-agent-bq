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
import { createRoot } from 'react-dom/client'
import { LineageGraph } from '../ui/LineageGraph'

export default function Home() {
  const [dashboards, setDashboards] = useState<any[]>([])
  const [activeId, setActiveId] = useState<string>('')
  const [active, setActive] = useState<any | null>(null)
  const [rowsByKpi, setRowsByKpi] = useState<Record<string, any[]>>({})
  const [localFilters, setLocalFilters] = useState<{ from?: string; to?: string; category?: { column?: string; value?: string } }>({})
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true)
  const [toasts, setToasts] = useState<{ id: number; type: 'success'|'error'|'info'; msg: string; title?: string }[]>([])
  const toast = (type: 'success'|'error'|'info', msg: string, title?: string) => {
    const id = Date.now() + Math.random()
    setToasts(t => [...t, { id, type, msg, title }])
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
  const [cxoTyping, setCxoTyping] = useState(false)
  const chatWrapRef = useRef<HTMLDivElement | null>(null)
  const [chatPos, setChatPos] = useState<{ x: number; y: number }>({ x: 16, y: 16 })
  const [chatSize, setChatSize] = useState<{ w: number; h: number }>({ w: 520, h: 0 })
  const feedLayerRef = useRef<HTMLDivElement | null>(null)
  const [exportOpen, setExportOpen] = useState(false)
  const exportDropdownRef = useRef<HTMLDivElement>(null)

  // Lineage modal state
  const [lineageOpen, setLineageOpen] = useState(false)
  const [lineageKpi, setLineageKpi] = useState<any | null>(null)
  const [lineageData, setLineageData] = useState<any | null>(null)
  const [lineageLoading, setLineageLoading] = useState(false)
  const [lineageError, setLineageError] = useState<string | null>(null)

  // Initial dashboard loading - only run once
  useEffect(() => { 
    (async () => {
      try {
        // First, fetch the dashboard list
        await refreshDashboardList()
        
        // Then, try to load the most recent dashboard
        const mostRecent = await api.getMostRecentDashboard()
        if (mostRecent) {
          loadDashboard(mostRecent, false) // Don't show refresh toast on initial load
        } else if (dashboards.length > 0) {
          // If no recent dashboard exists, load the first available one
          loadDashboard(dashboards[0].id, false) // Don't show refresh toast on initial load
        }
      } catch (error) {
        console.error('Failed to load most recent dashboard:', error)
        // Fallback to first dashboard if available
        if (dashboards.length > 0) {
          loadDashboard(dashboards[0].id, false) // Don't show refresh toast on initial load
        }
      }
    })()
  }, []) // Only run once on mount

  // Close export dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (exportDropdownRef.current && !exportDropdownRef.current.contains(event.target as Node)) {
        setExportOpen(false)
      }
    }

    if (exportOpen) {
      document.addEventListener('mousedown', handleClickOutside)
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [exportOpen])

  useEffect(() => {
    const el = gridWrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setGridW(el.clientWidth))
    ro.observe(el)
    setGridW(el.clientWidth)
    return () => ro.disconnect()
  }, [gridWrapRef.current])

  async function loadDashboard(id: string, showRefreshToast: boolean = true) {
    setActiveId(id)
    const d = await api.getDashboard(id)
    setActive(d)
    setRowsByKpi({})
    setTabs((d.tabs && d.tabs.length ? d.tabs : [{ id: 'overview', name: 'Overview', order: 0 }]))
    setActiveTab(d.last_active_tab || 'overview')
    if (d.global_filters && d.global_filters.date) { setLocalFilters({ from: d.global_filters.date.from, to: d.global_filters.date.to }) } else { setLocalFilters({}) }
    
    if (showRefreshToast) {
      setTimeout(() => refreshAll(d), 0)
    } else {
      // Just run KPIs without showing toast for initial load
      setTimeout(async () => {
        const dash = d
        if (!dash) return
        for (const k of dash.kpis || []) {
          await runKpiWithFilters(k, false) // No toast for initial load
        }
      }, 0)
    }
  }

  async function runKpiWithFilters(kpi: any, showToast: boolean = false) {
    const filters: any = {}
    if (localFilters.from || localFilters.to) filters.date = { from: localFilters.from, to: localFilters.to }
    if (localFilters.category && localFilters.category.column && localFilters.category.value) filters.category = localFilters.category
    
    try {
      const res = await api.runKpi(kpi.sql, filters, kpi.filter_date_column, kpi.expected_schema)
      setRowsByKpi(prev => ({ ...prev, [kpi.id]: res }))
      
      if (showToast) {
        toast('success', `${kpi.name} updated`, 'KPI Refresh')
      }
    } catch (error) {
      console.error(`Failed to run KPI ${kpi.name}:`, error)
      if (showToast) {
        toast('error', `Failed to update ${kpi.name}`, 'KPI Error')
      }
    }
  }

  async function refreshAll(d?: any) {
    const dash = d || active
    if (!dash) return
    
    toast('info', `Refreshing ${dash.name}...`, 'Dashboard Update')
    
          try {
        for (const k of dash.kpis || []) {
          await runKpiWithFilters(k, false) // No individual KPI toasts during bulk refresh
        }
        toast('success', `${dash.name} refreshed successfully`, 'Dashboard Updated')
      } catch (error) {
        console.error('Failed to refresh dashboard:', error)
        toast('error', `Failed to refresh ${dash.name}`, 'Refresh Error')
      }
  }

  async function openLineage(k: any) {
    try {
      setLineageOpen(true)
      setLineageKpi(k)
      setLineageLoading(true)
      setLineageError(null)
      const res = await api.getLineage(k.sql)
      setLineageData(res)
    } catch (e: any) {
      setLineageError(String(e?.response?.data?.detail?.message || e?.message || e))
      setLineageData({ sources: [], joins: [], graph: { nodes: [], edges: [] } })
    } finally {
      setLineageLoading(false)
    }
  }

  async function refreshDashboardList() {
    try {
      const rows = await api.listDashboards()
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
    } catch (error) {
      console.error('Failed to refresh dashboard list:', error)
    }
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
    setCxoTyping(true)
    const context = {
      dashboard_name: active.name,
      active_tab: activeTab,
      kpis: (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: rowsByKpi[k.id] || [] }))
    }
    const reply = await api.cxoSend(convId, msg, context)
    setChat(prev => [...prev, { role: 'assistant', text: reply }])
    setCxoTyping(false)
  }

  function getPrimaryColor(): string {
    const c = getComputedStyle(document.documentElement).getPropertyValue('--primary').trim()
    return c || '#239BA7'
  }

  function drawBrandHeader(doc: jsPDF, title: string, sub?: string) {
    const primary = getPrimaryColor()
    // brand bar
    doc.setFillColor(primary)
    doc.rect(0, 0, 210, 12, 'F')
    // title
    doc.setFont('helvetica', 'bold')
    doc.setTextColor(0, 0, 0)
    doc.setFontSize(18)
    doc.text(title, 14, 22)
    if (sub) {
      doc.setFont('helvetica', 'normal')
      doc.setFontSize(11)
      doc.text(sub, 14, 30)
    }
    doc.setTextColor(0, 0, 0)
    // divider
    doc.setDrawColor(220)
    doc.line(14, 34, 196, 34)
  }

  function drawFooter(doc: jsPDF, page: number) {
    const primary = getPrimaryColor()
    const link = 'https://analytics-kpi-poc-315425729064.asia-south1.run.app'
    doc.setDrawColor(230)
    doc.line(14, 285, 196, 285)
    doc.setFont('helvetica', 'normal')
    doc.setFontSize(9)
    doc.setTextColor(0, 0, 238)
    doc.textWithLink('Open Dashboard', 14, 292, { url: link })
    doc.setTextColor(0, 0, 0)
    doc.text(`Page ${page}`, 196 - 18, 292, { align: 'right' as any })
  }

  function drawSectionHeader(doc: jsPDF, y: number, text: string) {
    const primary = getPrimaryColor()
    doc.setFillColor(primary)
    doc.rect(14, y - 4, 3, 10, 'F')
    doc.setFont('helvetica', 'bold')
    doc.setFontSize(13)
    doc.text(text, 20, y + 3)
  }

  async function captureChartImage(card: HTMLElement): Promise<HTMLCanvasElement | null> {
    const target = card.querySelector('.no-drag') as HTMLElement
    if (!target) return null
    try {
      return await html2canvas(target, { backgroundColor: '#ffffff', scale: 2 })
    } catch { return null }
  }

  async function renderKpiOffscreen(kpi: any, rows: any[]): Promise<HTMLCanvasElement | null> {
    return new Promise(async (resolve) => {
      try {
        const holder = document.createElement('div')
        holder.style.position = 'fixed'
        holder.style.left = '-10000px'
        holder.style.top = '0px'
        holder.style.width = '800px'
        holder.style.height = '360px'
        holder.style.background = '#ffffff'
        document.body.appendChild(holder)
        const root = createRoot(holder)
        root.render(React.createElement(ChartRenderer, { chart: kpi, rows }))
        await new Promise(r => setTimeout(r, 700))
        const canvas = await html2canvas(holder, { backgroundColor: '#ffffff', scale: 2 })
        try { root.unmount() } catch {}
        try { document.body.removeChild(holder) } catch {}
        resolve(canvas)
      } catch {
        resolve(null)
      }
    })
  }

  // Enhanced markdown processing with support for **bold** and *italic* formatting
  function renderMarkdownText(doc: jsPDF, text: string, x: number, y: number, maxWidth: number): { height: number } {
    // Parse inline markdown: **bold**, *italic* 
    const parts = []
    let processedText = text
    
    // Process **bold** and *italic* patterns
    const patterns = [
      { regex: /\*\*(.*?)\*\*/g, style: 'bold' },
      { regex: /\*(.*?)\*/g, style: 'italic' }
    ]
    
    // Simple approach: parse bold first, then italic
    let currentPos = 0
    let matches = []
    
    // Find all formatting patterns
    patterns.forEach(pattern => {
      let match
      while ((match = pattern.regex.exec(text)) !== null) {
        matches.push({
          start: match.index,
          end: match.index + match[0].length,
          text: match[1],
          style: pattern.style,
          fullMatch: match[0]
        })
      }
    })
    
    // Sort matches by position
    matches.sort((a, b) => a.start - b.start)
    
    // Build parts array without overlapping matches
    currentPos = 0
    for (const match of matches) {
      // Skip overlapping matches (prefer the first one)
      if (match.start < currentPos) continue
      
      // Add text before the formatted part
      if (match.start > currentPos) {
        parts.push({ text: text.substring(currentPos, match.start), style: 'normal' })
      }
      
      // Add the formatted part
      parts.push({ text: match.text, style: match.style })
      currentPos = match.end
    }
    
    // Add remaining text
    if (currentPos < text.length) {
      parts.push({ text: text.substring(currentPos), style: 'normal' })
    }
    
    // If no formatting found, treat as single normal text
    if (parts.length === 0) {
      parts.push({ text: text, style: 'normal' })
    }
    
    // Render the parts with improved word wrapping
    let currentX = x
    let currentY = y
    const lineHeight = 6
    
    for (const part of parts) {
      if (!part.text) continue
      
      // Set font based on formatting (note: jsPDF has limited italic support)
      const fontWeight = part.style === 'bold' ? 'bold' : 'normal'
      doc.setFont('helvetica', fontWeight)
      doc.setFontSize(11)
      
      // Split into words for proper wrapping
      const words = part.text.split(' ')
      
      for (let i = 0; i < words.length; i++) {
        const word = words[i]
        const space = i < words.length - 1 ? ' ' : ''
        const wordWithSpace = word + space
        const wordWidth = doc.getTextWidth(wordWithSpace)
        
        // Check if word fits on current line
        if (currentX + wordWidth > x + maxWidth && currentX > x) {
          currentY += lineHeight
          currentX = x
        }
        
        // Render the word
        doc.text(word, currentX, currentY)
        currentX += doc.getTextWidth(word)
        
        // Add space if not the last word
        if (space) {
          currentX += doc.getTextWidth(' ')
        }
      }
    }
    
    return { height: Math.max(lineHeight, currentY - y + lineHeight) }
  }

  function addSummaryPage(doc: jsPDF, dashboardName: string | undefined, mdText: string) {
    drawBrandHeader(doc, 'CXO Summary', new Date().toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }))
    // Enhanced markdown rendering with support for **bold**, *italic*, headers, and bullets
    const lines = (mdText || '').split(/\r?\n/)
    let y = 46
    const margin = 14
    const maxWidth = 180
    
    for (const raw of lines) {
      let line = raw.trim()
      if (!line) { y += 4; continue }
      
      if (line.startsWith('###')) {
        doc.setFont('helvetica', 'bold'); doc.setFontSize(12)
        const cleanText = line.replace(/^###\s*/, '').replace(/\*\*(.*?)\*\*/g, '$1') // Remove ** from headers
        doc.text(cleanText, margin, y)
        y += 8
      } else if (line.startsWith('##')) {
        const cleanText = line.replace(/^##\s*/, '').replace(/\*\*(.*?)\*\*/g, '$1') // Remove ** from headers
        drawSectionHeader(doc, y, cleanText)
        y += 12
      } else if (line.startsWith('#')) {
        doc.setFont('helvetica', 'bold'); doc.setFontSize(14)
        const cleanText = line.replace(/^#\s*/, '').replace(/\*\*(.*?)\*\*/g, '$1') // Remove ** from headers
        doc.text(cleanText, margin, y)
        y += 10
      } else if (line.match(/^[-*•]\s+/)) {
        const bulletText = '• ' + line.replace(/^[-*•]\s+/, '')
        const result = renderMarkdownText(doc, bulletText, margin, y, maxWidth)
        y += result.height
      } else if (line.match(/^\d+\.\s+/)) {
        // Handle numbered lists (1. 2. 3. etc.)
        const result = renderMarkdownText(doc, line, margin, y, maxWidth)
        y += result.height
      } else {
        const result = renderMarkdownText(doc, line, margin, y, maxWidth)
        y += result.height
      }
      
      // Check for page break
      if (y > 260) {
        const nextPage = ((doc as any).getNumberOfPages?.() || 1) + 1
        drawFooter(doc, nextPage - 1)
        doc.addPage(); drawBrandHeader(doc, 'CXO Summary – Continued')
        y = 46
      }
    }
    drawFooter(doc, (doc as any).getNumberOfPages?.() || 1)
  }

  async function exportCurrentDashboardPDF() {
    if (!active) return
    const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' })
    // Summary page (current tab)
    const context = {
      dashboard_name: active.name,
      active_tab: activeTab,
      kpis: (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: rowsByKpi[k.id] || [] }))
    }
    const id = convId || await api.cxoStart(active.id, active.name, activeTab)
    const summary = await api.cxoSend(id, 'Generate executive summary from available data.', context)
    addSummaryPage(doc, active.name, summary)

    // Charts pages (2 per page vertically) with captions
    const cards = Array.from(document.querySelectorAll('.layout .card')) as HTMLElement[]
    const visible = (active.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTab) : activeTab === 'overview'))
    const imgs: { title: string; dataUrl: string; }[] = []
    for (const k of visible) {
      const card = cards.find(c => (c.getAttribute('data-grid') || '').includes(`\"i\":\"${k.id}\"`)) || cards.find(c => c.innerText.includes(k.name))
      if (!card) continue
      const canvas = await captureChartImage(card)
      if (!canvas) continue
      imgs.push({ title: k.name, dataUrl: canvas.toDataURL('image/png') })
    }
    if (imgs.length) {
      doc.addPage(); drawBrandHeader(doc, 'Charts')
      let slot = 0
      for (let i = 0; i < imgs.length; i++) {
        const { title, dataUrl } = imgs[i]
        const ySlots = [46, 165]
        const y = ySlots[slot % 2]
        const imgW = 180
        const imgH = 100
        doc.addImage(dataUrl, 'PNG', 15, y, imgW, imgH)
        doc.setFont('helvetica', 'normal'); doc.setFontSize(10)
        doc.text(title, 15, y + imgH + 6)
        if (slot % 2 === 1 && i < imgs.length - 1) { drawFooter(doc, (doc as any).getNumberOfPages?.() || 1); doc.addPage(); drawBrandHeader(doc, 'Charts') }
        slot++
      }
      drawFooter(doc, (doc as any).getNumberOfPages?.() || 1)
    }
    // finalize
    doc.save(`${active.name || 'dashboard'}-cxo-summary.pdf`)
  }

  async function exportAllDashboardsPDF() {
    const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' })
    let firstSection = true
    for (const d of dashboards) {
      const full = await api.getDashboard(d.id)
      const activeTabId = (full.last_active_tab || 'overview')
      // latest summary
      const context = {
        dashboard_name: full.name,
        active_tab: activeTabId,
        kpis: (full.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTabId) : activeTabId === 'overview')).map((k:any) => ({ id: k.id, name: k.name, rows: [] }))
      }
      const cid = await api.cxoStart(full.id, full.name, activeTabId)
      const summary = await api.cxoSend(cid, 'Generate executive summary from available data.', context)
      if (!firstSection) doc.addPage()
      addSummaryPage(doc, full.name, `# ${full.name}\n\n${summary}`)

      // charts offscreen (current tab)
      const visible = (full.kpis || []).filter((k:any) => (Array.isArray(k.tabs) && k.tabs.length ? k.tabs.includes(activeTabId) : activeTabId === 'overview'))
      const imgs: { title: string; dataUrl: string; }[] = []
      for (const k of visible) {
        const rows = await api.runKpi(k.sql, undefined, k.filter_date_column, k.expected_schema)
        const canvas = await renderKpiOffscreen(k, rows)
        if (!canvas) continue
        imgs.push({ title: k.name, dataUrl: canvas.toDataURL('image/png') })
      }
      if (imgs.length) {
        doc.addPage(); drawBrandHeader(doc, `${full.name} – Charts`)
        let slot = 0
        for (let i = 0; i < imgs.length; i++) {
          const { title, dataUrl } = imgs[i]
          const ySlots = [46, 165]
          const y = ySlots[slot % 2]
          const imgW = 180
          const imgH = 100
          doc.addImage(dataUrl, 'PNG', 15, y, imgW, imgH)
          doc.setFont('helvetica', 'normal'); doc.setFontSize(10)
          doc.text(title, 15, y + imgH + 6)
          if (slot % 2 === 1 && i < imgs.length - 1) { drawFooter(doc, (doc as any).getNumberOfPages?.() || 1); doc.addPage(); drawBrandHeader(doc, `${full.name} – Charts`) }
          slot++
        }
        drawFooter(doc, (doc as any).getNumberOfPages?.() || 1)
      }
      firstSection = false
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
        {toasts.map(t => (
          <div key={t.id} className={`toast toast-${t.type}`} style={{
            background: t.type === 'success' ? '#10b981' : 
                        t.type === 'error' ? '#ef4444' : 
                        '#3b82f6',
            color: '#ffffff',
            padding: '12px 16px',
            borderRadius: '8px',
            marginBottom: '8px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            border: 'none',
            fontSize: '14px',
            fontWeight: '500',
            maxWidth: '400px',
            wordWrap: 'break-word'
          }}>
            {t.title && (
              <div style={{ 
                fontWeight: '600', 
                marginBottom: '4px', 
                fontSize: '12px',
                opacity: '0.9'
              }}>
                {t.title}
              </div>
            )}
            <div>{t.msg}</div>
          </div>
        ))}
      </div>
      <div className="topbar header-gradient" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', position: 'sticky', top: 0, zIndex: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button className="btn btn-ghost" onClick={() => setSidebarOpen(o => !o)} title={sidebarOpen ? 'Collapse' : 'Expand'}>|||</button>
          <div style={{ fontSize: 18, fontWeight: 700 }}>Dashboards</div>
          {active && <span className="badge">{active.name} v{active.version}</span>}
        </div>
        <div className="toolbar">
          <button className="btn btn-accent" onClick={openCxo}>CXO AI Assist</button>
          <div style={{ position: 'relative' }} ref={exportDropdownRef}>
            <button className="btn" onClick={() => {
              const newState = !exportOpen
              console.log('Export dropdown state:', newState)
              setExportOpen(newState)
            }} style={{ 
              background: exportOpen ? 'var(--primary)' : undefined,
              color: exportOpen ? '#fff' : undefined
            }}>
              Export CXO Summary {exportOpen ? '▴' : '▾'}
            </button>
            {exportOpen && (
              <div 
                style={{ 
                  position: 'absolute', 
                  right: 0, 
                  top: '110%', 
                  background: '#ffffff', 
                  border: '2px solid #e0e0e0', 
                  boxShadow: '0 6px 20px rgba(0,0,0,0.2)', 
                  borderRadius: 8, 
                  zIndex: 1000,
                  minWidth: '200px',
                  overflow: 'hidden'
                }}
                onMouseEnter={() => console.log('Dropdown hovered')}
              >
                <button 
                  className="btn" 
                  onClick={() => { 
                    console.log('Current Dashboard PDF clicked')
                    setExportOpen(false); 
                    exportCurrentDashboardPDF() 
                  }} 
                  style={{ 
                    display: 'block', 
                    width: '100%', 
                    textAlign: 'left',
                    padding: '12px 16px',
                    border: 'none',
                    borderBottom: '1px solid #f0f0f0',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '14px',
                    color: '#333',
                    transition: 'background-color 0.2s ease'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = '#f5f5f5'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                >
                  Current Dashboard (PDF)
                </button>
                <button 
                  className="btn" 
                  onClick={() => { 
                    console.log('All Dashboards PDF clicked')
                    setExportOpen(false); 
                    exportAllDashboardsPDF() 
                  }} 
                  style={{ 
                    display: 'block', 
                    width: '100%', 
                    textAlign: 'left',
                    padding: '12px 16px',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '14px',
                    color: '#333',
                    transition: 'background-color 0.2s ease'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = '#f5f5f5'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                >
                  All Dashboards (PDF)
                </button>
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
                {cxoTyping && (
                  <div style={{ marginBottom: 12, textAlign: 'left' }}>
                    <div className="typing"><span className="dot"></span><span className="dot"></span><span className="dot"></span></div>
                  </div>
                )}
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
                  <div key={d.id} style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 6 }}>
                    <button className="btn" onClick={() => loadDashboard(d.id)} style={{ display: 'flex', width: '100%', justifyContent: 'space-between', alignItems: 'center', background: d.id === active?.id ? 'linear-gradient(90deg, rgba(35,155,167,0.25), rgba(122,218,165,0.25))' : 'linear-gradient(90deg, var(--surface), rgba(122,218,165,0.2))', borderColor: d.id === active?.id ? 'var(--primary)' : undefined }}>
                      <span style={{ textAlign: 'left' }}>
                        <div className="card-title">{d.name}</div>
                        <div className="card-subtitle">v{d.version}</div>
                      </span>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        {d.default_flag && <span className="chip" style={{ background: 'var(--primary)', color: '#fff', fontSize: '10px' }}>Default</span>}
                        {d.id === active?.id ? <span className="chip">Selected</span> : <span className="chip">View</span>}
                      </div>
                    </button>

                  </div>
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
                        <button className="btn btn-sm" onClick={() => runKpiWithFilters(k, true)}>Refresh</button>
                        <button className="btn btn-sm" onClick={() => openLineage(k)}>Lineage</button>
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

      {lineageOpen && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9999 }} onClick={() => setLineageOpen(false)}>
          <div 
            style={{ 
              position: 'absolute', left: '50%', top: '50%', width: '58.5%', height: '58.5%', transform: 'translate(-50%, -50%)',
              background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 12, boxShadow: 'var(--shadow)'
            }}
            onClick={e => e.stopPropagation()}
          >
            <div className="card-header" style={{ padding: 12 }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'baseline' }}>
                <div className="card-title">KPI Lineage</div>
                {lineageKpi?.name && <div className="card-subtitle">{lineageKpi.name}</div>}
              </div>
              <div className="toolbar">
                <button className="btn btn-sm" onClick={() => setLineageOpen(false)}>✕</button>
              </div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 12, padding: 12, height: 'calc(100% - 56px)' }}>
              <div className="panel" style={{ height: '100%', padding: 0, overflow: 'hidden' }}>
                {lineageLoading ? (
                  <div style={{ padding: 16 }}>Loading lineage…</div>
                ) : lineageError ? (
                  <div style={{ padding: 16, color: 'var(--danger)' }}>Failed to load lineage: {lineageError}</div>
                ) : (
                  <div style={{ position: 'relative', width: '100%', height: '100%' }}>
                    <LineageGraph graph={(lineageData && lineageData.graph) || { nodes: [], edges: [] }} joins={(lineageData && lineageData.joins) || []} outputs={(lineageData && lineageData.outputs) || undefined} />
                  </div>
                )}
              </div>
              <div className="panel" style={{ height: '100%', overflow: 'auto' }}>
                <div className="section-title">Overview</div>
                <div className="card-subtitle">ID: {lineageKpi?.id}</div>
                <div className="card-subtitle">Schema: {lineageKpi?.expected_schema}</div>
                <div className="card-subtitle">Chart: {lineageKpi?.chart_type}</div>
                {lineageKpi?.filter_date_column && <div className="card-subtitle">Filter Date Column: {lineageKpi.filter_date_column}</div>}

                <div className="section-title" style={{ marginTop: 12 }}>Sources</div>
                <div className="scroll">
                  {(lineageData?.sources || []).length ? (
                    (lineageData?.sources || []).map((s: string) => (
                      <div key={s} className="list-item"><span style={{ flex: 1 }}>{s}</span></div>
                    ))
                  ) : (
                    <div className="card-subtitle">No sources detected</div>
                  )}
                </div>

                <div className="section-title" style={{ marginTop: 12 }}>Joins</div>
                <div className="scroll">
                  {(lineageData?.joins || []).length ? (
                    (lineageData?.joins || []).map((j: any, idx: number) => (
                      <div key={idx} className="list-item">
                        <div className="card-subtitle">{j.type || 'JOIN'}: {j.left_table} ↔ {j.right_table}</div>
                        {j.on && <div style={{ fontSize: 12, color: 'var(--muted)' }}>ON {j.on}</div>}
                        {(j.pairs || []).length > 0 && (
                          <div style={{ fontSize: 12 }}>
                            {(j.pairs || []).map((p: any, i: number) => (
                              <div key={i}>{p.left} = {p.right}</div>
                            ))}
                          </div>
                        )}
                      </div>
                    ))
                  ) : (
                    <div className="card-subtitle">No joins found</div>
                  )}
                </div>

                <div className="section-title" style={{ marginTop: 12 }}>Filters</div>
                {(lineageData?.filters || []).length ? (
                  (lineageData?.filters || []).map((f: string, idx: number) => (
                    <div key={idx} className="list-item"><span style={{ flex: 1 }}>{f}</span></div>
                  ))
                ) : (
                  <div className="card-subtitle">No filters</div>
                )}

                <div className="section-title" style={{ marginTop: 12 }}>Group By / Outputs</div>
                {lineageData?.groupBy && lineageData.groupBy.length > 0 && (
                  <div className="card-subtitle">Group By: {lineageData.groupBy.join(', ')}</div>
                )}
                {lineageData?.outputs && (
                  <div className="card-subtitle">Outputs: {Object.entries(lineageData.outputs).filter(([,v]) => Boolean(v)).map(([k,v]) => `${k}: ${(v as string).replace(/\s+/g,' ')}`).join(' | ')}</div>
                )}

                <div className="section-title" style={{ marginTop: 12 }}>Raw JSON</div>
                <div className="toolbar" style={{ marginBottom: 8 }}>
                  <button className="btn btn-sm" onClick={() => { try { navigator.clipboard.writeText(JSON.stringify(lineageData, null, 2)) } catch {} }}>Copy JSON</button>
                </div>
                <pre style={{ maxHeight: 220, overflow: 'auto', fontSize: 11 }}><code>{JSON.stringify(lineageData, null, 2)}</code></pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}