import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'

type Props = {
  datasets: any[]
  onChange: (selected: {datasetId: string, tableId: string}[]) => void
}

export function TableSelector({ datasets, onChange }: Props) {
  const [activeDataset, setActiveDataset] = useState<string>('')
  const [tablesByDataset, setTablesByDataset] = useState<Record<string, any[]>>({})
  const [selectedByDataset, setSelectedByDataset] = useState<Record<string, Set<string>>>({})
  const [loadingTables, setLoadingTables] = useState(false)
  const [error, setError] = useState<string>('')

  useEffect(() => {
    if (datasets.length && !activeDataset) {
      setActiveDataset(datasets[0].datasetId)
    }
  }, [datasets])

  useEffect(() => {
    async function loadTables(dsId: string) {
      if (!dsId) return
      if (tablesByDataset[dsId]) return
      setLoadingTables(true)
      setError('')
      try {
        const tables = await api.getTables(dsId)
        setTablesByDataset(prev => ({ ...prev, [dsId]: tables }))
      } catch (e: any) {
        setError('Failed to load tables. Please check permissions.')
      } finally {
        setLoadingTables(false)
      }
    }
    loadTables(activeDataset)
  }, [activeDataset])

  useEffect(() => {
    const out: {datasetId: string, tableId: string}[] = []
    for (const dsId of Object.keys(selectedByDataset)) {
      for (const tbl of Array.from(selectedByDataset[dsId] || [])) {
        out.push({ datasetId: dsId, tableId: tbl })
      }
    }
    onChange(out)
  }, [selectedByDataset])

  const activeTables = useMemo(() => tablesByDataset[activeDataset] || [], [tablesByDataset, activeDataset])
  const activeSelected = useMemo(() => selectedByDataset[activeDataset] || new Set<string>(), [selectedByDataset, activeDataset])

  function toggle(tableId: string) {
    setSelectedByDataset(prev => {
      const dsSet = new Set(prev[activeDataset] || [])
      if (dsSet.has(tableId)) dsSet.delete(tableId)
      else dsSet.add(tableId)
      return { ...prev, [activeDataset]: dsSet }
    })
  }

  function selectAll() {
    const allIds = activeTables.map(t => t.tableId)
    setSelectedByDataset(prev => ({ ...prev, [activeDataset]: new Set(allIds) }))
  }

  function clearAll() {
    setSelectedByDataset(prev => ({ ...prev, [activeDataset]: new Set() }))
  }

  return (
    <div>
      <div style={{ marginBottom: 8 }}>
        <div className="section-title">Dataset</div>
        <select
          className="select"
          value={activeDataset}
          onChange={e => setActiveDataset(e.target.value)}
          disabled={!datasets.length}
        >
          {datasets
            .filter(ds => !ds.isBackendCreated) // Filter out backend-created datasets
            .map(ds => (
              <option key={ds.datasetId} value={ds.datasetId}>
                {ds.datasetId}
                {ds.isBackendCreated && <span style={{ color: '#999', fontStyle: 'italic' }}> (Backend)</span>}
              </option>
            ))}
        </select>
      </div>

      {error && <div className="badge" style={{ marginBottom: 8, borderColor: 'crimson', color: 'crimson', background: 'rgba(220,20,60,0.06)' }}>{error}</div>}

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <div className="section-title" style={{ margin: 0 }}>Tables</div>
        <div className="toolbar">
          <button className="btn btn-sm" onClick={selectAll} disabled={!activeTables.length || loadingTables}>Select All</button>
          <button className="btn btn-sm" onClick={clearAll} disabled={!activeSelected.size}>Clear</button>
        </div>
      </div>

      <div className="scroll">
        {loadingTables && <div>Loading tables...</div>}
        {!loadingTables && activeTables.length === 0 && <div>No tables found for this dataset.</div>}
        {!loadingTables && activeTables.map(t => (
          <label key={t.tableId} className="list-item" style={{ gap: 8 }}>
            <input
              type="checkbox"
              checked={activeSelected.has(t.tableId)}
              onChange={() => toggle(t.tableId)}
            />
            <span style={{ flex: 1 }}>{t.tableId}</span>
            {activeSelected.has(t.tableId) && <span className="tag">selected</span>}
          </label>
        ))}
      </div>
    </div>
  )
}