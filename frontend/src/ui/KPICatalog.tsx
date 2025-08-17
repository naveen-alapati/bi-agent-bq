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
      <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
        <input placeholder="datasetId" value={datasetId} onChange={e => setDatasetId(e.target.value)} />
        <input placeholder="tableId" value={tableId} onChange={e => setTableId(e.target.value)} />
        <input placeholder="search..." value={q} onChange={e => setQ(e.target.value)} style={{ flex: 1 }} />
        <button onClick={load}>Refresh</button>
      </div>
      <div style={{ maxHeight: 320, overflow: 'auto', border: '1px solid #eee', padding: 6 }}>
        {filtered.map(it => (
          <div key={it.id} style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid #f2f2f2', padding: '6px 0' }}>
            <div>
              <div style={{ fontWeight: 600 }}>{it.name}</div>
              <div style={{ color: '#666', fontSize: 12 }}>{it.dataset_id}.{it.table_id}</div>
            </div>
            <button onClick={() => onAdd(it)}>Add</button>
          </div>
        ))}
      </div>
    </div>
  )
}