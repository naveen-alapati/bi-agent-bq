import React, { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'

export function KPICatalog({ onAdd }: { onAdd: (item: any) => void }) {
  const [datasetId, setDatasetId] = useState('')
  const [tableId, setTableId] = useState('')
  const [items, setItems] = useState<any[]>([])
  const [q, setQ] = useState('')

  async function load() {
    const rows = await api.listKpiCatalog({ datasetId: datasetId || undefined, tableId: tableId || undefined })
    setItems(rows)
  }

  useEffect(() => {
    load().catch(() => {})
  }, [datasetId, tableId])

  const filtered = useMemo(() => {
    if (!q) return items
    const qq = q.toLowerCase()
    return items.filter(i => (i.name || '').toLowerCase().includes(qq) || (i.sql || '').toLowerCase().includes(qq) || (i.table_id || '').toLowerCase().includes(qq))
  }, [q, items])

  return (
    <div>
      <div className="toolbar" style={{ marginBottom: 8 }}>
        <input className="input" placeholder="datasetId" value={datasetId} onChange={e => setDatasetId(e.target.value)} />
        <input className="input" placeholder="tableId" value={tableId} onChange={e => setTableId(e.target.value)} />
        <input className="input" placeholder="search..." value={q} onChange={e => setQ(e.target.value)} style={{ flex: 1 }} />
        <button className="btn" onClick={load}>Refresh</button>
      </div>
      <div className="scroll">
        {filtered.map(it => (
          <div key={it.id} className="list-item">
            <div>
              <div className="card-title">{it.name}</div>
              <div className="card-subtitle">{it.dataset_id}.{it.table_id}</div>
            </div>
            <button className="btn btn-sm" onClick={() => onAdd(it)}>Add</button>
          </div>
        ))}
      </div>
    </div>
  )
}