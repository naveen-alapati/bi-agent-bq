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

  // pick first dataset by default when list becomes available
  useEffect(() => {
    if (datasets.length && !activeDataset) {
      setActiveDataset(datasets[0].datasetId)
    }
  }, [datasets])

  // load tables for active dataset on change
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

  // emit selection whenever selectedByDataset changes
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
        <label style={{ display: 'block', marginBottom: 4 }}>Dataset</label>
        <select
          value={activeDataset}
          onChange={e => setActiveDataset(e.target.value)}
          style={{ width: '100%' }}
          disabled={!datasets.length}
        >
          {datasets.map(ds => (
            <option key={ds.datasetId} value={ds.datasetId}>{ds.datasetId}</option>
          ))}
        </select>
      </div>

      {error && <div style={{ color: 'crimson', marginBottom: 8 }}>{error}</div>}

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <div style={{ fontWeight: 600 }}>Tables</div>
        <div>
          <button onClick={selectAll} disabled={!activeTables.length || loadingTables} style={{ marginRight: 6 }}>Select All</button>
          <button onClick={clearAll} disabled={!activeSelected.size}>Clear</button>
        </div>
      </div>

      <div style={{ maxHeight: 320, overflow: 'auto', border: '1px solid #eee', padding: 8 }}>
        {loadingTables && <div>Loading tables...</div>}
        {!loadingTables && activeTables.length === 0 && <div>No tables found for this dataset.</div>}
        {!loadingTables && activeTables.map(t => (
          <label key={t.tableId} style={{ display: 'block' }}>
            <input
              type="checkbox"
              checked={activeSelected.has(t.tableId)}
              onChange={() => toggle(t.tableId)}
            /> {t.tableId}
          </label>
        ))}
      </div>
    </div>
  )
}