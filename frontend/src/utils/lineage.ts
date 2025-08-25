export interface LineageJoin {
  left: string
  right: string
  on: string
}

export interface LineageOutputs {
  x?: string
  y?: string
  label?: string
  value?: string
}

export interface Lineage {
  sources: string[]
  joins: LineageJoin[]
  filters?: string[]
  groupBy?: string[]
  outputs?: LineageOutputs
  filterDateColumn?: string
}

function unique<T>(arr: T[]): T[] {
  return Array.from(new Set(arr))
}

export function computeKpiLineage(sql: string, kpi?: any): Lineage {
  const text = String(sql || '')
  const sources: string[] = []
  const joins: LineageJoin[] = []
  let filters: string[] | undefined
  let groupBy: string[] | undefined
  const outputs: LineageOutputs = {}
  let filterDateColumn: string | undefined = kpi?.filter_date_column

  try {
    const backticked = [...text.matchAll(/`([\w-]+\.[\w-]+\.[\w-]+)`/g)].map(m => m[1])
    sources.push(...backticked)
    const simpleFromJoin = [...text.matchAll(/\b(?:from|join)\s+([\w-]+\.[\w-]+)(?:\s+|\b)/ig)].map(m => m[1])
    sources.push(...simpleFromJoin)
  } catch {}

  try {
    const joinRegex = /join\s+[`]?([\w-]+\.[\w-]+\.[\w-]+)[`]?[^]*?\bon\s+([^\n\r]+?)(?=\n|\r|\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)/ig
    let m: RegExpExecArray | null
    while ((m = joinRegex.exec(text)) !== null) {
      const on = (m[2] || '').trim()
      const eq = on.match(/([\w`.]+)\s*=\s*([\w`.]+)/)
      if (eq) {
        joins.push({ left: eq[1].replace(/[`]/g, ''), right: eq[2].replace(/[`]/g, ''), on })
      } else {
        joins.push({ left: '', right: '', on })
      }
    }
  } catch {}

  try {
    const w = text.match(/\bwhere\b([\s\S]*?)(?=\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)/i)
    if (w && w[1]) {
      const raw = w[1].trim()
      filters = raw.split(/\band\b/ig).map(s => s.trim()).filter(Boolean)
    }
  } catch {}

  try {
    const g = text.match(/\bgroup\s+by\b([\s\S]*?)(?=\border\s+by\b|\blimit\b|$)/i)
    if (g && g[1]) {
      groupBy = g[1].split(',').map(s => s.trim()).filter(Boolean)
    }
  } catch {}

  try {
    const sel = text.match(/\bselect\b([\s\S]*?)\bfrom\b/i)
    if (sel && sel[1]) {
      const s = sel[1]
      const a = (name: string) => {
        const rx = new RegExp(`\\bas\\s+${name}\\b`, 'i')
        const m = s.split(',').map(x => x.trim()).find(part => rx.test(part))
        return m || undefined
      }
      outputs.x = a('x')
      outputs.y = a('y')
      outputs.label = a('label')
      outputs.value = a('value')
      if (!filterDateColumn) {
        const dateAlias = s.match(/\bdate\s*\(.*?\)\s+as\s+([\w_]+)/i)
        if (dateAlias) filterDateColumn = dateAlias[1]
      }
    }
  } catch {}

  const out: Lineage = {
    sources: unique(sources).filter(Boolean),
    joins,
  }
  if (filters && filters.length) out.filters = filters
  if (groupBy && groupBy.length) out.groupBy = groupBy
  if (outputs && Object.values(outputs).some(Boolean)) out.outputs = outputs
  if (filterDateColumn) out.filterDateColumn = filterDateColumn
  return out
}

