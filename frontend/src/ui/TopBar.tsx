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
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 12px', borderBottom: '1px solid #eee', position: 'sticky', top: 0, background: 'var(--bg, #fff)', zIndex: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <input value={name} onChange={e => onNameChange(e.target.value)} placeholder="dashboard name" style={{ fontSize: 16, fontWeight: 600 }} />
        {version && <span style={{ color: '#666' }}>v{version}</span>}
        <button onClick={onSave}>Save</button>
        <button onClick={onSaveAs}>Save As</button>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <div>
          <label style={{ fontSize: 12, color: '#666', display: 'block' }}>Date</label>
          <div>
            <input type="date" value={globalDate.from || ''} onChange={e => onGlobalDateChange({ ...globalDate, from: e.target.value })} />
            <span style={{ margin: '0 6px' }}>to</span>
            <input type="date" value={globalDate.to || ''} onChange={e => onGlobalDateChange({ ...globalDate, to: e.target.value })} />
          </div>
        </div>
        <button onClick={onExportDashboard}>Export CSV</button>
        <button onClick={onThemeToggle}>{theme === 'light' ? 'Dark' : 'Light'} Theme</button>
      </div>
    </div>
  )
}