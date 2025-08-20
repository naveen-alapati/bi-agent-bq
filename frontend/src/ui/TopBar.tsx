import React, { useState, useEffect } from 'react'

export function TopBar({
  name,
  version,
  onNameChange,
  onSave,
  globalDate,
  onGlobalDateChange,
  theme,
  onThemeToggle,
  onExportDashboard,
  onToggleSidebar,
  sidebarOpen,
  dirty,
  dashboardId,
}: {
  name: string
  version?: string
  onNameChange: (v: string) => void
  onSave: () => void
  globalDate: { from?: string; to?: string }
  onGlobalDateChange: (next: { from?: string; to?: string }) => void
  theme: 'light' | 'dark'
  onThemeToggle: () => void
  onExportDashboard: () => void
  onToggleSidebar?: () => void
  sidebarOpen?: boolean
  dirty?: boolean
  dashboardId?: string
}) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(name)
  useEffect(() => { setDraft(name) }, [name])

  const startEdit = () => { setDraft(name); setEditing(true) }
  const commit = () => { setEditing(false); if (draft !== name) onNameChange(draft) }
  const cancel = () => { setEditing(false); setDraft(name) }

  return (
    <div className="topbar header-gradient" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', position: 'sticky', top: 0, zIndex: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <button className="btn btn-ghost" onClick={onToggleSidebar} title={sidebarOpen ? 'Collapse' : 'Expand'}>|||</button>
        <a className="btn btn-ghost" href="/">Home</a>
        {editing ? (
          <input
            className="input"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            autoFocus
            onBlur={commit}
            onKeyDown={e => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') cancel() }}
            style={{ fontSize: 16, fontWeight: 600 }}
          />
        ) : (
          <span className="name-badge" style={{ cursor: 'text' }} onDoubleClick={startEdit} title="Double-click to rename">{name || 'Untitled'}</span>
        )}
        {version && <span className="badge">v{version}</span>}
        {dirty ? (
          <button className="btn btn-primary" onClick={onSave}>Save</button>
        ) : null}
      </div>
      <div className="toolbar" style={{ alignItems: 'center' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input className="input" type="date" value={globalDate.from || ''} onChange={e => onGlobalDateChange({ ...globalDate, from: e.target.value })} />
          <span>to</span>
          <input className="input" type="date" value={globalDate.to || ''} onChange={e => onGlobalDateChange({ ...globalDate, to: e.target.value })} />
        </div>
        <button className="btn" onClick={onExportDashboard}>Export CSV</button>
        <button className="btn" onClick={onThemeToggle}>{theme === 'light' ? 'Dark' : 'Light'} Theme</button>
      </div>
    </div>
  )
}