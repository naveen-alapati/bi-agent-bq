import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'

type TableRef = { datasetId: string; tableId: string }

export function ThoughtGraphPicker({ selected }: { selected: TableRef[] }) {
	const datasetIds = useMemo(() => Array.from(new Set(selected.map(s => s.datasetId))), [selected])
	const [options, setOptions] = useState<{ id: string; name: string; version?: string; primary_dataset_id?: string }[]>([])
	const [value, setValue] = useState<string>('')

	useEffect(() => {
		if (datasetIds.length !== 1) { setOptions([]); setValue(''); return }
		api.listThoughtGraphs(datasetIds[0]).then(setOptions).catch(()=>{ setOptions([]) })
	}, [datasetIds.join('|')])

	if (datasetIds.length !== 1) return (
		<div className="card-subtitle">Select one dataset to pick a Thought Graph</div>
	)

	return (
		<div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
			<select className="select" value={value} onChange={e => setValue(e.target.value)}>
				<option value="">Select Thought Graph…</option>
				{options.map(o => (<option key={o.id} value={o.id}>{o.name} (v{o.version})</option>))}
			</select>
		</div>
	)
}

