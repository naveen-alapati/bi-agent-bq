import React, { useEffect, useMemo, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { api as apiClient } from '../services/api'

export type TableRef = { datasetId: string; tableId: string }

type Draft = any

type Props = {
  open: boolean
  tables: TableRef[]
  drafts: Draft[]
  globalDate?: { from?: string; to?: string }
  onClose: () => void
  onFinalized: (inserted: number) => void
}

export function KPIDraftsModal({ open, tables, drafts, globalDate, onClose, onFinalized }: Props) {
  const [search, setSearch] = useState('')
  const [items, setItems] = useState<Draft[]>([])
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [validating, setValidating] = useState(false)
  const [finalizing, setFinalizing] = useState(false)
  const [testingById, setTestingById] = useState<Record<string, boolean>>({})
  const [issuesById, setIssuesById] = useState<Record<string, { type: string; message: string }[]>>({})
  const [aiInputById, setAiInputById] = useState<Record<string, string>>({})
  const [aiChatById, setAiChatById] = useState<Record<string, { role: 'assistant'|'user'; text: string }[]>>({})
  const [aiOpenById, setAiOpenById] = useState<Record<string, boolean>>({})
  const [aiTypingById, setAiTypingById] = useState<Record<string, boolean>>({})
  const [originalById, setOriginalById] = useState<Record<string, Draft>>({})

  useEffect(() => {
    const cloned = (drafts || []).map((d: any) => ({ ...d }))
    setItems(cloned)
    setSelectedIds(new Set((drafts || []).map((d: any) => d.id)))
    setIssuesById({})
    // capture originals for restore/modified tracking
    const originals: Record<string, Draft> = {}
    for (const d of cloned) originals[d.id] = { ...d }
    setOriginalById(originals)
    setAiOpenById({})
  }, [drafts, open])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return items
    return items.filter(it => (
      (it.name || '').toLowerCase().includes(q) ||
      (it.sql || '').toLowerCase().includes(q) ||
      (it.expected_schema || '').toLowerCase().includes(q) ||
      (it.chart_type || '').toLowerCase().includes(q)
    ))
  }, [items, search])

  function isModifiedAgainstOriginal(id: string, candidate: Draft) {
    const orig = originalById[id]
    if (!orig) return false
    const keys = ['name','sql','chart_type','expected_schema','filter_date_column']
    for (const k of keys) {
      if ((orig as any)[k] !== (candidate as any)[k]) return true
    }
    return false
  }

  function toggleSelect(id: string) {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function updateItem(id: string, patch: Partial<Draft>) {
    setItems(prev => prev.map(it => {
      if (it.id !== id) return it
      const next = { ...it, ...patch }
      const modified = isModifiedAgainstOriginal(id, next)
      return { ...next, _modified: modified }
    }))
  }

  async function validateSelected() {
    const toValidate = items.filter(it => selectedIds.has(it.id))
    if (!toValidate.length) return
    setValidating(true)
    try {
      const results = await apiClient.kpiDraftsValidate(tables, toValidate)
      const nextIssues: Record<string, { type: string; message: string }[]> = {}
      for (const r of results) {
        nextIssues[r.id] = r.issues || []
      }
      setIssuesById(nextIssues)
    } finally {
      setValidating(false)
    }
  }

  async function validateOne(it: Draft) {
    try {
      const [r] = await apiClient.kpiDraftsValidate(tables, [it])
      setIssuesById(prev => ({ ...prev, [it.id]: (r && r.issues) || [] }))
    } catch {}
  }

  async function testSql(it: Draft) {
    setTestingById(prev => ({ ...prev, [it.id]: true }))
    try {
      const rows = await apiClient.runKpi(it.sql, { date: globalDate }, it.filter_date_column, it.expected_schema)
      const cnt = Array.isArray(rows) ? rows.length : 0
      const msg = cnt > 0 ? `Test OK – ${cnt} rows` : 'Test OK – 0 rows'
      alert(msg)
    } catch (e: any) {
      alert(`Test failed: ${e?.message || e}`)
    } finally {
      setTestingById(prev => ({ ...prev, [it.id]: false }))
    }
  }

  function toggleAi(it: Draft) {
    setAiOpenById(prev => ({ ...prev, [it.id]: !prev[it.id] }))
    const history = aiChatById[it.id] || []
    if (!history.length) {
      setAiChatById(prev => ({
        ...prev,
        [it.id]: [{ role: 'assistant', text: "Let's refine this KPI. Tell me what you'd like to change (chart type, labels, SQL, grouping, filters)." }]
      }))
    }
  }

  async function sendAi(it: Draft) {
    const text = (aiInputById[it.id] || '').trim()
    if (!text) return
    const history = aiChatById[it.id] || []
    setAiChatById(prev => ({ ...prev, [it.id]: [...history, { role: 'user', text }] }))
    setAiInputById(prev => ({ ...prev, [it.id]: '' }))
    setAiTypingById(prev => ({ ...prev, [it.id]: true }))
    try {
      const res = await apiClient.editKpiChat(it, text, history.map(m => ({ role: m.role, content: m.text })))
      if (res.reply) setAiChatById(prev => ({ ...prev, [it.id]: [...(prev[it.id] || []), { role: 'assistant', text: res.reply }] }))
      if (res.kpi) {
        const updated = { ...it, ...res.kpi }
        updateItem(it.id, { ...res.kpi })
        await validateOne(updated)
      }
    } catch (e: any) {
      alert(`AI error: ${e?.message || e}`)
    } finally {
      setAiTypingById(prev => ({ ...prev, [it.id]: false }))
    }
  }

  function restoreOriginal(id: string) {
    const orig = originalById[id]
    if (!orig) return
    setItems(prev => prev.map(it => it.id === id ? { ...orig, _modified: false } : it))
    setIssuesById(prev => ({ ...prev, [id]: [] }))
    setAiChatById(prev => ({ ...prev, [id]: [] }))
    setAiInputById(prev => ({ ...prev, [id]: '' }))
  }

  async function finalizeSelected() {
    const toFinalize = items.filter(it => selectedIds.has(it.id) && !it._removed)
    if (!toFinalize.length) return
    setFinalizing(true)
    try {
      const res = await apiClient.kpiDraftsFinalize(toFinalize)
      onFinalized(res.inserted || 0)
      onClose()
    } catch (e: any) {
      alert(`Finalize failed: ${e?.message || e}`)
    } finally {
      setFinalizing(false)
    }
  }

  if (!open) return null

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9999 }}>
      <div 
        style={{ 
          position: 'absolute',
          left: '50%',
          top: '50%',
          transform: 'translate(-50%, -50%)',
          width: 'min(960px, 96vw)', 
          height: 'min(78vh, 90vh)', 
          background: 'var(--card)', 
          border: '1px solid var(--border)', 
          borderRadius: 12, 
          display: 'flex', 
          flexDirection: 'column',
          boxShadow: 'var(--shadow)'
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: 14, borderBottom: '1px solid var(--border)' }}>
          <div className="card-title">KPI Drafts Review</div>
          <div className="toolbar">
            <input className="input" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)} style={{ width: 220, marginRight: 8 }} />
            <button className="btn btn-sm" onClick={() => setSelectedIds(new Set(items.map(i => i.id)))}>Select All</button>
            <button className="btn btn-sm" onClick={() => setSelectedIds(new Set())}>None</button>
            <button className="btn btn-sm" onClick={validateSelected} disabled={validating}>{validating ? 'Validating...' : 'Validate'}</button>
            <button className="btn btn-primary btn-sm" onClick={finalizeSelected} disabled={finalizing || !items.some(i => selectedIds.has(i.id))}>{finalizing ? 'Finalizing...' : 'Finalize to Catalog'}</button>
            <button className="btn btn-sm" onClick={onClose}>✕</button>
          </div>
        </div>
        <div style={{ flex: 1, overflow: 'auto', padding: 12, display: 'grid', gap: 10 }}>
          {filtered.map((it) => {
            const issues = issuesById[it.id] || []
            const hasErrors = issues.length > 0
            const tablePrefix = (it.id || '').split(':')[0]
            const conf = typeof it.confidence_score === 'number' ? it.confidence_score : undefined
            const confColor = conf === undefined ? '#999' : conf >= 0.8 ? '#10b981' : conf >= 0.6 ? '#f59e0b' : '#ef4444'
            return (
              <div key={it.id} className="card" style={{ padding: 10, borderColor: hasErrors ? 'crimson' : undefined }}>
                <div className="card-header" style={{ marginBottom: 8 }}>
                  <div>
                    <div className="card-title" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <input type="checkbox" checked={selectedIds.has(it.id)} onChange={() => toggleSelect(it.id)} />
                      <input className="input" value={it.name} onChange={e => updateItem(it.id, { name: e.target.value })} style={{ maxWidth: 420 }} />
                      {conf !== undefined && (
                        <span className="chip" title="Confidence score" style={{ background: confColor, color: '#fff' }}>{Math.round(conf * 100)}%</span>
                      )}
                      {it._modified && (
                        <span className="tag" title="Modified">Modified</span>
                      )}
                    </div>
                    <div className="card-subtitle" style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                      <span>{tablePrefix}</span>
                      <select className="select" value={it.chart_type} onChange={e => updateItem(it.id, { chart_type: e.target.value })}>
                        <option value="line">line</option>
                        <option value="bar">bar</option>
                        <option value="pie">pie</option>
                        <option value="area">area</option>
                        <option value="scatter">scatter</option>
                      </select>
                      <select className="select" value={it.expected_schema} onChange={e => updateItem(it.id, { expected_schema: e.target.value })}>
                        <option value="timeseries">timeseries</option>
                        <option value="categorical">categorical</option>
                        <option value="distribution">distribution</option>
                        <option value="scatter">scatter</option>
                      </select>
                    </div>
                  </div>
                  <div className="card-actions">
                    <button className="btn btn-sm" onClick={() => testSql(it)} disabled={!!testingById[it.id]}>{testingById[it.id] ? 'Testing...' : 'Test SQL'}</button>
                    <button className="btn btn-sm" onClick={() => updateItem(it.id, { _removed: true })}>Remove</button>
                    <button className="btn btn-sm" onClick={() => toggleAi(it)}>{aiOpenById[it.id] ? 'Hide AI' : 'AI Edit'}</button>
                    <button className="btn btn-sm" onClick={() => restoreOriginal(it.id)} disabled={!it._modified}>Restore Original</button>
                  </div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                  <div>
                    <div className="card-subtitle" style={{ marginBottom: 6 }}>SQL</div>
                    <textarea className="input" value={it.sql} onChange={e => updateItem(it.id, { sql: e.target.value })} style={{ width: '100%', minHeight: 120, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }} />
                    {hasErrors && (
                      <div style={{ marginTop: 8, padding: 10, background: 'var(--surface)', borderRadius: 8, border: '1px solid var(--border)' }}>
                        <div className="card-subtitle" style={{ marginBottom: 6 }}>Issues</div>
                        <div className="scroll" style={{ maxHeight: 160 }}>
                          <ul style={{ margin: 0, paddingLeft: 18 }}>
                            {issues.map((iss, idx) => (
                              <li key={idx} style={{ color: iss.type === 'sql' ? 'crimson' : undefined }}>{iss.type}: {iss.message}</li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    )}
                  </div>
                  <div>
                    {aiOpenById[it.id] && (
                      <>
                        <div className="card-subtitle" style={{ marginBottom: 6 }}>AI Assist</div>
                        <div style={{ display: 'grid', gap: 6 }}>
                          <textarea className="input" placeholder="Ask AI to revise SQL/chart/filters..." value={aiInputById[it.id] || ''} onChange={e => setAiInputById(prev => ({ ...prev, [it.id]: e.target.value }))} style={{ width: '100%', minHeight: 72, resize: 'vertical' }} />
                          <button className="btn btn-sm" onClick={() => sendAi(it)} disabled={!!aiTypingById[it.id]}>{aiTypingById[it.id] ? 'Asking…' : 'Ask AI'}</button>
                          <div className="scroll" style={{ maxHeight: 220 }}>
                            {(aiChatById[it.id] || []).map((m, i) => (
                              <div key={i} style={{ marginBottom: 8 }}>
                                <div className="card-subtitle" style={{ marginBottom: 2 }}>{m.role === 'user' ? 'You' : 'Assistant'}</div>
                                {m.role === 'assistant' ? <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.text}</ReactMarkdown> : <div>{m.text}</div>}
                              </div>
                            ))}
                          </div>
                        </div>
                      </>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
          {filtered.filter(i => i._removed).length > 0 && (
            <div className="card" style={{ padding: 10 }}>
              <div className="card-subtitle">Removed ({filtered.filter(i => i._removed).length})</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8 }}>
                {filtered.filter(i => i._removed).map(i => (
                  <button key={i.id} className="btn btn-sm" onClick={() => updateItem(i.id, { _removed: false })}>Restore {i.name}</button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}