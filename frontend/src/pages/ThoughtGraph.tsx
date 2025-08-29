import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import { LineageGraph } from '../ui/LineageGraph'

type TableRef = { datasetId: string; tableId: string }

export default function ThoughtGraphPage() {
	const navigate = useNavigate()
	const location = useLocation() as any
	const initial = (location && (location.state as any)) || {}
	const [name, setName] = useState<string>(initial?.name || 'Thought Graph')
	const [selectedTables, setSelectedTables] = useState<TableRef[]>(() => {
		try {
			if (Array.isArray(initial?.selectedTables)) return initial.selectedTables
			const saved = sessionStorage.getItem('thoughtGraph')
			if (!saved) return []
			const parsed = JSON.parse(saved)
			return Array.isArray(parsed?.selectedTables) ? parsed.selectedTables : []
		} catch { return [] }
	})
	const [graph, setGraph] = useState<{ graph: { nodes: any[]; edges: any[] }; joins?: any[] }>(() => {
		try {
			if (initial?.graph) return initial.graph
			const saved = sessionStorage.getItem('thoughtGraph')
			if (!saved) return { graph: { nodes: [], edges: [] }, joins: [] }
			const parsed = JSON.parse(saved)
			return parsed?.graph || { graph: { nodes: [], edges: [] }, joins: [] }
		} catch { return { graph: { nodes: [], edges: [] }, joins: [] } }
	})
	const [loading, setLoading] = useState<boolean>(false)
	const [saving, setSaving] = useState<boolean>(false)
	const [datasetId, setDatasetId] = useState<string>(initial?.primary_dataset_id || '')
	const [datasets, setDatasets] = useState<any[]>([])
	const [graphsForDataset, setGraphsForDataset] = useState<any[]>([])
	const [selectedGraphId, setSelectedGraphId] = useState<string>('')

	useEffect(() => {
		api.getDatasets().then(setDatasets).catch(()=>{})
	}, [])

	useEffect(() => {
		try {
			sessionStorage.setItem('thoughtGraph', JSON.stringify({ name, selectedTables, graph }))
		} catch {}
	}, [name, selectedTables, graph])

	useEffect(() => {
		if (!datasetId) { setGraphsForDataset([]); return }
		api.listThoughtGraphs(datasetId).then(setGraphsForDataset).catch(()=>setGraphsForDataset([]))
	}, [datasetId])

	async function generateDraft() {
		if (!selectedTables.length) return
		setLoading(true)
		try {
			const res = await api.generateThoughtGraph(selectedTables, name)
			setGraph(res.graph)
		} finally { setLoading(false) }
	}

	async function saveGraph() {
		setSaving(true)
		try {
			const res = await api.saveThoughtGraph({ name, primary_dataset_id: datasetId || (selectedTables[0]?.datasetId || ''), datasets: Array.from(new Set(selectedTables.map(t=>t.datasetId))), selected_tables: selectedTables, graph })
			setSelectedGraphId(res.id)
			alert(`Saved Thought Graph ${res.name} v${res.version}`)
		} catch (e:any) {
			alert(`Failed to save: ${String(e?.response?.data?.detail || e?.message || e)}`)
		} finally { setSaving(false) }
	}

	function addNodeFromTable(t: TableRef) {
		const fq = `${initial?.projectId || ''}.${t.datasetId}.${t.tableId}`
		setGraph(prev => ({ ...prev, graph: { nodes: [...(prev.graph?.nodes || []), { id: fq, type: 'table', label: t.tableId }], edges: prev.graph?.edges || [] } }))
	}

	function removeNode(id: string) {
		setGraph(prev => ({ ...prev, graph: { nodes: (prev.graph?.nodes || []).filter((n:any)=>n.id!==id), edges: (prev.graph?.edges || []).filter((e:any)=> e.source!==id && e.target!==id) } }))
	}

	function addJoin(leftFq: string, rightFq: string) {
		setGraph(prev => ({ ...prev, joins: [ ...(prev.joins || []), { left_table: leftFq, right_table: rightFq, type: 'JOIN' } ] }))
	}

	const tablesInDataset = useMemo(() => {
		if (!datasetId) return []
		return selectedTables.filter(t => t.datasetId === datasetId)
	}, [datasetId, selectedTables])

	return (
		<div className="app-grid" style={{ gridTemplateColumns: 'var(--sidebar-w, 320px) 1fr' }}>
			<div className="sidebar" style={{ display: 'grid', gap: 12 }}>
				<div className="panel">
					<div className="section-title">Thought Graph</div>
					<div className="toolbar" style={{ gap: 8 }}>
						<input className="input" placeholder="Name" value={name} onChange={e=>setName(e.target.value)} />
						<button className="btn" onClick={() => navigate(-1)}>Back</button>
					</div>
				</div>
				<div className="panel">
					<div className="section-title">Dataset</div>
					<select className="select" value={datasetId} onChange={e=>setDatasetId(e.target.value)}>
						<option value="">Select dataset…</option>
						{datasets.map((d:any)=> (<option key={d.datasetId} value={d.datasetId}>{d.datasetId}</option>))}
					</select>
					{datasetId && (
						<div style={{ marginTop: 8 }}>
							<div className="card-subtitle">Existing Thought Graphs</div>
							<select className="select" value={selectedGraphId} onChange={async e => {
								const id = e.target.value
								setSelectedGraphId(id)
								if (!id) return
								try {
									const full = await api.getThoughtGraph(id)
									setName(full.name)
									setSelectedTables(full.selected_tables || [])
									setGraph(full.graph)
								} catch {}
							}}>
								<option value="">Select existing…</option>
								{graphsForDataset.map((g:any)=> (<option key={g.id} value={g.id}>{g.name} (v{g.version})</option>))}
							</select>
						</div>
					)}
				</div>
				<div className="panel">
					<div className="section-title">Selected Tables</div>
					<div className="card-subtitle">Add/remove nodes and define joins.</div>
					<div className="scroll">
						{selectedTables.map((t, idx) => (
							<div key={`${t.datasetId}.${t.tableId}.${idx}`} className="list-item">
								<div style={{ flex: 1 }}>{t.datasetId}.{t.tableId}</div>
								<div className="toolbar">
									<button className="btn btn-sm" onClick={() => addNodeFromTable(t)}>Add Node</button>
								</div>
							</div>
						))}
					</div>
					<div className="toolbar" style={{ marginTop: 8 }}>
						<button className="btn" onClick={generateDraft} disabled={!selectedTables.length || loading}>{loading ? 'Generating…':'Regenerate'}</button>
						<button className="btn btn-primary" onClick={saveGraph} disabled={saving}>{saving ? 'Saving…':'Publish'}</button>
					</div>
				</div>
			</div>
			<div className="panel" style={{ margin: 0, padding: 0, overflow: 'hidden' }}>
				<div style={{ height: '70vh' }}>
					<LineageGraph graph={graph.graph || { nodes: [], edges: [] }} joins={graph.joins || []} />
				</div>
				<div style={{ borderTop: '1px solid var(--border)', padding: 12 }}>
					<div className="toolbar" style={{ gap: 8, flexWrap: 'wrap' }}>
						<input className="input" placeholder="Left table FQN" id="__left" />
						<input className="input" placeholder="Right table FQN" id="__right" />
						<button className="btn btn-sm" onClick={() => {
							const l = (document.getElementById('__left') as HTMLInputElement)?.value?.trim()
							const r = (document.getElementById('__right') as HTMLInputElement)?.value?.trim()
							if (l && r) addJoin(l, r)
						}}>Add Join</button>
						<input className="input" placeholder="Node id to remove" id="__rm" />
						<button className="btn btn-sm" onClick={() => {
							const id = (document.getElementById('__rm') as HTMLInputElement)?.value?.trim()
							if (id) removeNode(id)
						}}>Remove Node</button>
					</div>
					<div className="card-subtitle" style={{ marginTop: 8 }}>Changes stay local until Publish. Use Regenerate to draft again.</div>
				</div>
			</div>
		</div>
	)
}

