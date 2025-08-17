import React, { useEffect, useMemo, useState } from 'react'
import { TableSelector } from '../ui/TableSelector'
import { KPIList } from '../ui/KPIList'
import { ChartCanvas } from '../ui/ChartCanvas'
import { api } from '../services/api'
import '../styles.css'
import GridLayout, { Layout, Layouts } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'

export default function App() {
	const [datasets, setDatasets] = useState<any[]>([])
	const [selected, setSelected] = useState<{datasetId: string, tableId: string}[]>([])
	const [kpis, setKpis] = useState<any[]>([])
	const [rowsByKpi, setRowsByKpi] = useState<Record<string, any[]>>({})
	const [loading, setLoading] = useState(false)
	const [loadError, setLoadError] = useState('')
	const [dashboardName, setDashboardName] = useState('ecom-v1')
	const [layouts, setLayouts] = useState<Layout[]>([])
	const [dashList, setDashList] = useState<any[]>([])
	const [saving, setSaving] = useState(false)

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
		} finally {
			setLoading(false)
		}
	}

	async function runKpi(kpi: any) {
		const res = await api.runKpi(kpi.sql)
		setRowsByKpi(prev => ({...prev, [kpi.id]: res}))
	}

	useEffect(() => {
		const defaultLayout = kpis.map((k, i) => ({ i: k.id, x: (i % 2) * 6, y: Math.floor(i / 2) * 8, w: 6, h: 8 }))
		setLayouts(defaultLayout)
	}, [kpis])

	function onLayoutChange(newLayout: Layout[]) {
		setLayouts(newLayout)
	}

	async function saveDashboard() {
		setSaving(true)
		try {
			const payload = { name: dashboardName, kpis, layout: layouts, selected_tables: selected }
			const res = await api.saveDashboard(payload)
			await api.listDashboards().then(setDashList)
			alert(`Saved dashboard ${res.name} (id: ${res.id})`)
		} finally {
			setSaving(false)
		}
	}

	async function loadDashboard(id: string) {
		const d = await api.getDashboard(id)
		setDashboardName(d.name)
		setKpis(d.kpis)
		setLayouts(d.layout)
		setSelected(d.selected_tables)
	}

	return (
		<div style={{ display: 'grid', gridTemplateColumns: '340px 1fr', gap: 16, padding: 16 }}>
			<div>
				<h3>Tables</h3>
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
					<h3>Dashboards</h3>
					<input value={dashboardName} onChange={e => setDashboardName(e.target.value)} placeholder="dashboard name" style={{ width: '100%', marginBottom: 8 }} />
					<button onClick={saveDashboard} disabled={saving || !kpis.length}>Save dashboard</button>
					<div style={{ marginTop: 8 }}>
						<select onChange={e => loadDashboard(e.target.value)} style={{ width: '100%' }}>
							<option value="">Load existing...</option>
							{dashList.map(d => (
								<option key={d.id} value={d.id}>{d.name}</option>
							))}
						</select>
					</div>
				</div>

				<div style={{ marginTop: 16 }}>
					<h3>KPIs</h3>
					<KPIList kpis={kpis} onRun={runKpi} />
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
								<button onClick={() => runKpi(k)} style={{ fontSize: 12 }}>Run</button>
							</div>
							<div style={{ flex: 1, padding: 8 }}>
								<ChartCanvas chart={k} rows={rowsByKpi[k.id] || []} />
							</div>
						</div>
					))}
				</GridLayout>
			</div>
		</div>
	)
}