import React, { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { api } from '../services/api'

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

	useEffect(() => {
		try {
			sessionStorage.setItem('kpiDrafts', JSON.stringify({ drafts, selectedTables }))
		} catch {}
	}, [drafts, selectedTables])

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
			setTesting(prev => ({ ...prev, [kpi.id]: { status: 'error', error: String(e?.response?.data?.detail || e?.message || e) } }))
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

	return (
		<div className="app-grid">
			<div className="panel" style={{ gridColumn: '1 / -1' }}>
				<div className="section-title">KPI Draft</div>
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
			</div>

			<div className="panel" style={{ gridColumn: '1 / -1' }}>
				<div className="scroll">
					{grouped.map(group => (
						<div key={`${group.datasetId}.${group.tableId}`} style={{ marginBottom: 16 }}>
							<div className="card-subtitle" style={{ marginBottom: 8 }}>{group.datasetId}.{group.tableId}</div>
							{group.items.map(k => (
								<div key={k.id} className="list-item" style={{ alignItems: 'flex-start', gap: 8 }}>
									<input type="checkbox" checked={!!selectedIds[k.id]} onChange={() => toggleSelect(k.id)} />
									<div style={{ flex: 1 }}>
										<div className="card-title">{k.name}</div>
										<div className="card-subtitle">Chart: {k.chart_type || 'bar'}</div>
									</div>
									<div className="toolbar">
										<button className="btn btn-sm" onClick={() => window.alert(k.sql)}>View SQL</button>
										<button className="btn btn-sm" onClick={() => testOne(k)} disabled={testing[k.id]?.status === 'loading'}>
											{testing[k.id]?.status === 'loading' ? 'Testing...' : 'Test SQL'}
										</button>
									</div>
									<div style={{ minWidth: 160, textAlign: 'right' }}>
										{testing[k.id]?.status === 'success' && (
											<span className="badge" style={{ background: 'var(--accent)', color: '#fff' }}>OK {testing[k.id]?.rows ?? 0} rows</span>
										)}
										{testing[k.id]?.status === 'error' && (
											<span className="badge" style={{ borderColor: 'crimson', color: 'crimson', background: 'rgba(220,20,60,0.06)' }}>Error</span>
										)}
									</div>
								</div>
							))}
						</div>
					))}
				</div>
			</div>
		</div>
	)
}