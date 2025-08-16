import React, { useEffect, useState } from 'react'
import { TableSelector } from '../ui/TableSelector'
import { KPIList } from '../ui/KPIList'
import { ChartCanvas } from '../ui/ChartCanvas'
import { api } from '../services/api'
import '../styles.css'

export default function App() {
	const [datasets, setDatasets] = useState<any[]>([])
	const [selected, setSelected] = useState<{datasetId: string, tableId: string}[]>([])
	const [kpis, setKpis] = useState<any[]>([])
	const [rowsByKpi, setRowsByKpi] = useState<Record<string, any[]>>({})
	const [loading, setLoading] = useState(false)
	const [loadError, setLoadError] = useState('')

	useEffect(() => {
		setLoadError('')
		api.getDatasets().then(setDatasets).catch(() => setLoadError('Failed to fetch datasets. Ensure the Cloud Run service account has BigQuery list permissions.'))
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
			</div>
			<div>
				<h3>KPIs</h3>
				<KPIList kpis={kpis} onRun={runKpi} />
				<div style={{ marginTop: 16 }}>
					<h3>Charts</h3>
					{kpis.map(k => (
						<div key={k.id} style={{ border: '1px solid #ddd', padding: 8, marginBottom: 12 }}>
							<div style={{ display: 'flex', justifyContent: 'space-between' }}>
								<div>
									<div style={{ fontWeight: 600 }}>{k.name}</div>
									<div style={{ color: '#666' }}>{k.short_description}</div>
								</div>
								<button onClick={() => runKpi(k)}>Run</button>
							</div>
							<ChartCanvas chart={k} rows={rowsByKpi[k.id] || []} />
						</div>
					))}
				</div>
			</div>
		</div>
	)
}