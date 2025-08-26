import React, { useEffect, useMemo, useState, useRef } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

export default function KPIDraft() {
	const navigate = useNavigate()
	const location = useLocation() as any
	const initial = location.state && (location.state as any)
	const [drafts, setDrafts] = useState<any[]>(() => {
		try {
			if (initial && Array.isArray(initial.drafts)) return initial.drafts
			const saved = sessionStorage.getItem('kpiDrafts')
			if (!saved) return []
			const parsed = JSON.parse(saved)
			return Array.isArray(parsed?.drafts) ? parsed.drafts : []
		} catch {
			return []
		}
	})
	const [selectedTables, setSelectedTables] = useState<{ datasetId: string; tableId: string }[]>(() => {
		try {
			if (initial && Array.isArray(initial.selectedTables)) return initial.selectedTables
			const saved = sessionStorage.getItem('kpiDrafts')
			if (!saved) return []
			const parsed = JSON.parse(saved)
			return Array.isArray(parsed?.selectedTables) ? parsed.selectedTables : []
		} catch {
			return []
		}
	})
	const [selectedIds, setSelectedIds] = useState<Record<string, boolean>>({})
	const [testing, setTesting] = useState<Record<string, { status: 'idle'|'loading'|'success'|'error'; rows?: number; error?: string }>>({})
	const [publishing, setPublishing] = useState(false)
	// Analyst chat state
	const [chatHistory, setChatHistory] = useState<{ role: 'user'|'assistant'; content: string }[]>([])
	const [chatInput, setChatInput] = useState('')
	const [chatLoading, setChatLoading] = useState(false)
	const [chatProposals, setChatProposals] = useState<any[] | null>(null)
	const chatScrollRef = useRef<HTMLDivElement | null>(null)
	const [autoAsked, setAutoAsked] = useState(false)
	const [autoAskLoading, setAutoAskLoading] = useState(false)

	useEffect(() => {
		try {
			sessionStorage.setItem('kpiDrafts', JSON.stringify({ drafts, selectedTables }))
		} catch {}
	}, [drafts, selectedTables])

	useEffect(() => {
		// Send initial message to Analyst with current drafts and selected tables
		if (chatHistory.length === 0 && drafts.length > 0) {
			setTimeout(() => {
				void sendChat("We have generated the following KPIs. Please propose high-value cross-table KPIs with runnable BigQuery SQL using provided table schemas and sample rows. If joins are insufficient, specify required keys.")
			}, 0)
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [])

	useEffect(() => {
		const el = chatScrollRef.current
		if (el) el.scrollTop = el.scrollHeight
	}, [chatHistory, chatLoading])

	// Auto-ask analyst for proposals when coming from Add KPI flow (no drafts yet but tables selected)
	useEffect(() => {
		if (autoAsked) return
		if (drafts.length === 0 && Array.isArray(selectedTables) && selectedTables.length > 0) {
			setAutoAskLoading(true)
			setAutoAsked(true)
			;(async () => {
				try {
					const prompt = "Propose 3–5 high-impact cross-table KPIs with runnable BigQuery SQL using the selected tables. If joins are insufficient, list missing keys per KPI."
					const res = await api.analystChat(prompt, drafts, selectedTables, [], true)
					setChatHistory(prev => [...prev, { role: 'user', content: prompt }])
					if (res.reply) setChatHistory(prev => [...prev, { role: 'assistant', content: res.reply }])
					if (Array.isArray(res.kpis) && res.kpis.length) {
						setChatProposals(res.kpis)
					} else {
						// Fallback: generate proposals directly if chat returned no structured KPIs
						try { await api.prepare(selectedTables, 5) } catch {}
						try {
							const more = await api.generateKpis(selectedTables, 5, true)
							setChatProposals(more)
						} catch {}
					}
				} catch (e) {}
				finally { setAutoAskLoading(false) }
			})()
		}
	}, [drafts, selectedTables, autoAsked])

	function parseSources(k: any): { cross: boolean; sources: string[] } {
		try {
			const idPrefix = (k.id || '').split(':')[0] || ''
			const parts = (k.sql || '').match(/`([\w-]+\.[\w-]+\.[\w-]+)`/g) || []
			const uniq = Array.from(new Set(parts.map((p: string) => p.replace(/[`]/g, ''))))
			const cross = (k.id || '').includes(':cross_') || uniq.length > 1
			const sources = uniq.length ? uniq : [idPrefix]
			return { cross, sources }
		} catch {
			return { cross: false, sources: [] }
		}
	}

	const grouped = useMemo(() => {
		const map: Record<string, { datasetId: string; tableId: string; items: any[] }> = {}
		for (const k of drafts) {
			const prefix = (k.id || '').split(':')[0] || ''
			const [datasetId, tableId] = prefix.split('.')
			const key = `${datasetId}.${tableId}`
			if (!map[key]) map[key] = { datasetId, tableId, items: [] }
			map[key].items.push(k)
		}
		return Object.values(map).sort((a,b)=>a.datasetId.localeCompare(b.datasetId)||a.tableId.localeCompare(b.tableId))
	}, [drafts])

	function toggleSelect(id: string) {
		setSelectedIds(prev => ({ ...prev, [id]: !prev[id] }))
	}

	function selectAll(v: boolean) {
		const next: Record<string, boolean> = {}
		for (const g of grouped) {
			for (const it of g.items) next[it.id] = v
		}
		setSelectedIds(next)
	}

	async function testOne(kpi: any) {
		setTesting(prev => ({ ...prev, [kpi.id]: { status: 'loading' } }))
		try {
			const rows = await api.runKpi(kpi.sql, undefined, kpi.filter_date_column, kpi.expected_schema)
			setTesting(prev => ({ ...prev, [kpi.id]: { status: 'success', rows: rows?.length || 0 } }))
		} catch (e: any) {
			const errDetail = e?.response?.data?.detail || e?.message || e
			setTesting(prev => ({ ...prev, [kpi.id]: { status: 'error', error: String(e?.response?.data?.detail || e?.message || e) } }))
			// Feed failure to AI Analyst to propose a fix
			try {
				const msg = `Test failed for KPI "${kpi.name}". Here is the structured error and SQL. Please provide a fixed SQL that preserves required aliases.\n\nError JSON:\n\n\n\`${JSON.stringify(errDetail, null, 2)}\`\n\nSQL:\n\n\n\`${kpi.sql}\``
				const res = await api.analystChat(msg, drafts, selectedTables, chatHistory, true)
				setChatHistory(prev => [...prev, { role: 'user', content: msg }])
				if (res.reply) setChatHistory(prev => [...prev, { role: 'assistant', content: res.reply }])
				if (Array.isArray(res.kpis) && res.kpis.length) setChatProposals(res.kpis)
			} catch {}
		}
	}

	async function publishSelected() {
		const toPublish: Record<string, { datasetId: string; tableId: string; items: any[] }> = {}
		for (const g of grouped) {
			for (const it of g.items) {
				if (!selectedIds[it.id]) continue
				const key = `${g.datasetId}.${g.tableId}`
				if (!toPublish[key]) toPublish[key] = { datasetId: g.datasetId, tableId: g.tableId, items: [] }
				toPublish[key].items.push(it)
			}
		}
		const groups = Object.values(toPublish)
		if (!groups.length) return
		setPublishing(true)
		try {
			for (const gr of groups) {
				await api.addToKpiCatalog(gr.datasetId, gr.tableId, gr.items)
			}
			try { sessionStorage.removeItem('kpiDrafts') } catch {}
			window.alert(`Published ${groups.reduce((s, g) => s + g.items.length, 0)} KPI(s) to catalog`)
			navigate('/editor')
		} catch (e: any) {
			window.alert(`Failed to publish: ${String(e?.response?.data?.detail || e?.message || e)}`)
		} finally {
			setPublishing(false)
		}
	}

	async function sendChat(message?: string) {
		const msg = (message ?? chatInput).trim()
		if (!msg) return
		setChatInput('')
		setChatLoading(true)
		setChatHistory(prev => [...prev, { role: 'user', content: msg }])
		try {
			const res = await api.analystChat(msg, drafts, selectedTables, chatHistory, true)
			setChatHistory(prev => [...prev, { role: 'assistant', content: res.reply }])
			if (Array.isArray(res.kpis) && res.kpis.length) {
				setChatProposals(res.kpis)
			}
		} catch (e: any) {
			setChatHistory(prev => [...prev, { role: 'assistant', content: `Sorry, I hit an error: ${String(e?.response?.data?.detail || e?.message || e)}` }])
		} finally {
			setChatLoading(false)
		}
	}

	function addProposalToDrafts(k: any) {
		setDrafts(prev => [...prev, k])
		setChatProposals(ps => (Array.isArray(ps) ? ps.filter(x => x.id !== k.id) : null))
	}

	function removeDraftToProposals(k: any) {
		setDrafts(prev => prev.filter(d => d.id !== k.id))
		setSelectedIds(prev => { const copy = { ...prev }; try { delete copy[k.id] } catch {} return copy })
		setChatProposals(ps => {
			const arr = Array.isArray(ps) ? ps : []
			const without = arr.filter(x => x.id !== k.id)
			return [...without, k]
		})
	}

	async function analyzeSimilar() {
		try {
			await api.prepare(selectedTables, 5)
			const more = await api.generateKpis(selectedTables, 5, true)
			setDrafts(prev => [...prev, ...more])
		} catch (e) {}
	}

	return (
		<div className="app-grid" style={{ gridTemplateColumns: 'var(--sidebar-w, 320px) 1fr' }}>
			<div style={{ display: 'grid', gap: 12 }}>
				<div className="panel">
					<div className="card-subtitle" style={{ marginBottom: 8 }}>All Draft KPIs</div>
					<div className="scroll">
						{drafts.map(k => (
							<div key={k.id} className="list-item">
								<div style={{ flex: 1 }}>
									<div className="card-title" style={{ fontSize: 13 }}>{k.name}</div>
									<div className="card-subtitle" style={{ fontSize: 12, opacity: 0.8 }}>{(k.id||'').split(':')[0]}</div>
								</div>
								<div className="toolbar" style={{ gap: 6, alignItems: 'center' }}>
									<button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View</button>
									<button className="btn btn-sm" onClick={() => removeDraftToProposals(k)}>Remove</button>
								</div>
							</div>
						))}
					</div>
				</div>
			</div>

			<div className="sidebar" style={{ display: 'grid', gap: 12 }}>
				<div className="panel">
					<div className="section-title">Proposed KPIs</div>
					<div className="toolbar" style={{ gap: 8 }}>
						<button className="btn" onClick={() => navigate('/editor')}>Back to Editor</button>
						<button className="btn" onClick={() => selectAll(true)}>Select All</button>
						<button className="btn" onClick={() => selectAll(false)}>Clear</button>
						<button className="btn btn-primary" onClick={publishSelected} disabled={publishing}>
							{publishing ? 'Publishing...' : 'Publish Selected to KPI Catalog'}
						</button>
						<div className="card-subtitle" style={{ marginLeft: 'auto' }}>
							Selected: {Object.values(selectedIds).filter(Boolean).length} / {drafts.length}
						</div>
					</div>
					<div className="scroll" style={{ marginTop: 8 }}>
						{Array.isArray(chatProposals) && chatProposals.length > 0 ? (
							chatProposals.map((k: any) => (
								<div key={k.id} className="list-item">
									<div style={{ flex: 1 }}>
										<div className="card-title">{k.name}</div>
										<div className="card-subtitle">Chart: {k.chart_type}</div>
									</div>
																	<div className="toolbar">
									<button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View SQL</button>
									<button className="btn btn-sm" onClick={() => addProposalToDrafts(k)}>Add to Drafts</button>
									<button className="btn btn-sm" onClick={() => testOne(k)} disabled={testing[k.id]?.status === 'loading'}>Test</button>
									{testing[k.id]?.status === 'success' && <span style={{ color: 'green', fontSize: 12 }}>OK</span>}
									{testing[k.id]?.status === 'error' && <span style={{ color: 'crimson', fontSize: 12 }}>Error</span>}
								</div>
								</div>
							))
						) : (
							<div className="card-subtitle">No proposals yet. Ask the analyst to propose cross-table KPIs.</div>
						)}
					</div>
					<div className="toolbar" style={{ marginTop: 8 }}>
						<button className="btn" onClick={analyzeSimilar}>Analyze similar</button>
					</div>
				</div>

				<div className="panel">
					<div className="section-title">AI Analyst</div>
					<div style={{ display: 'grid', gap: 8 }}>
						<div className="card-subtitle">Ask for cross-table KPIs or guidance. The analyst sees your generated KPIs and tables.</div>
						<div ref={chatScrollRef} style={{ maxHeight: 240, overflow: 'auto', border: '1px solid var(--border)', borderRadius: 8, padding: 8 }}>
							{chatHistory.map((m, i) => (
								<div key={i} style={{ marginBottom: 8 }}>
									<div className="card-subtitle" style={{ marginBottom: 4 }}>{m.role === 'user' ? 'You' : 'Analyst'}</div>
									<ReactMarkdown remarkPlugins={[remarkGfm]}>{m.content}</ReactMarkdown>
								</div>
							))}
							{chatLoading && <div className="typing"><span className="dot"></span><span className="dot"></span><span className="dot"></span></div>}
						</div>
						<div className="toolbar">
							<textarea
								className="input"
								placeholder="Ask the analyst..."
								value={chatInput}
								onChange={e => setChatInput(e.target.value)}
								onKeyDown={e => { if (e.key==='Enter' && !e.shiftKey) { e.preventDefault(); sendChat() } }}
								rows={4}
								style={{ flex: 1, minHeight: 150, resize: 'vertical' }}
							/>
							<button className="btn btn-primary" onClick={() => sendChat()} disabled={chatLoading}>Send</button>
						</div>
					</div>
				</div>
			</div>
		</div>
	)
}