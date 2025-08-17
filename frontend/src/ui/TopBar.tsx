import React from 'react'

export function TopBar({
  name,
  version,
  onNameChange,
  onSave,
  onSaveAs,
  globalDate,
  onGlobalDateChange,
  theme,
  onThemeToggle,
  onExportDashboard,
}: {
  name: string
  version?: string
  onNameChange: (v: string) => void
  onSave: () => void
  onSaveAs: () => void
  globalDate: { from?: string; to?: string }
  onGlobalDateChange: (next: { from?: string; to?: string }) => void
  theme: 'light' | 'dark'
  onThemeToggle: () => void
  onExportDashboard: () => void
}) {
  return (
    <div className="topbar header-gradient" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', position: 'sticky', top: 0, zIndex: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <input className="input" value={name} onChange={e => onNameChange(e.target.value)} placeholder="dashboard name" style={{ fontSize: 16, fontWeight: 600 }} />
        {version && <span className="badge">v{version}</span>}
        <button className="btn btn-primary" onClick={onSave}>Save</button>
        <button className="btn btn-ghost" onClick={onSaveAs}>Save As</button>
      </div>
      <div className="toolbar">
        <div>
          <label style={{ fontSize: 12, opacity: 0.9, display: 'block' }}>Date</label>
          <div>
            <input className="input" type="date" value={globalDate.from || ''} onChange={e => onGlobalDateChange({ ...globalDate, from: e.target.value })} />
            <span style={{ margin: '0 6px' }}>to</span>
            <input className="input" type="date" value={globalDate.to || ''} onChange={e => onGlobalDateChange({ ...globalDate, to: e.target.value })} />
          </div>
        </div>
        <button className="btn" onClick={onExportDashboard}>Export CSV</button>
        <button className="btn" onClick={onThemeToggle}>{theme === 'light' ? 'Dark' : 'Light'} Theme</button>
      </div>
    </div>
  )
}