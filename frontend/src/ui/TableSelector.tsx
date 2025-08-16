import React, { useEffect, useState } from 'react'
import { api } from '../services/api'

type Props = {
  datasets: any[]
  onChange: (selected: {datasetId: string, tableId: string}[]) => void
}

export function TableSelector({ datasets, onChange }: Props) {
  const [tablesByDataset, setTablesByDataset] = useState<Record<string, any[]>>({})
  const [selected, setSelected] = useState<Record<string, Set<string>>>({})

  useEffect(() => {
    async function load() {
      const all: Record<string, any[]> = {}
      for (const ds of datasets) {
        const tables = await api.getTables(ds.datasetId)
        all[ds.datasetId] = tables
      }
      setTablesByDataset(all)
    }
    if (datasets.length) load()
  }, [datasets])

  useEffect(() => {
    const out: {datasetId: string, tableId: string}[] = []
    for (const ds of Object.keys(selected)) {
      for (const t of Array.from(selected[ds] || [])) {
        out.push({ datasetId: ds, tableId: t })
      }
    }
    onChange(out)
  }, [selected])

  function toggle(dsId: string, tableId: string) {
    setSelected(prev => {
      const set = new Set(prev[dsId] || [])
      if (set.has(tableId)) set.delete(tableId)
      else set.add(tableId)
      return { ...prev, [dsId]: set }
    })
  }

  return (
    <div style={{ maxHeight: 500, overflow: 'auto', border: '1px solid #eee', padding: 8 }}>
      {datasets.map(ds => (
        <div key={ds.datasetId} style={{ marginBottom: 8 }}>
          <div style={{ fontWeight: 600 }}>{ds.datasetId}</div>
          <div>
            {(tablesByDataset[ds.datasetId] || []).map(t => (
              <label key={t.tableId} style={{ display: 'block' }}>
                <input type="checkbox" onChange={() => toggle(ds.datasetId, t.tableId)} /> {t.tableId}
              </label>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}