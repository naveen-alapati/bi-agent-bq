import axios from 'axios'

const RETRIEVAL_HEADER = 'X-Retrieval-Assist'

export function setRetrievalAssistEnabled(enabled: boolean) {
  if (enabled) {
    (axios.defaults.headers.common as any)[RETRIEVAL_HEADER] = 'true'
  } else {
    try { delete (axios.defaults.headers.common as any)[RETRIEVAL_HEADER] } catch {}
  }
  try { localStorage.setItem('retrievalAssist', JSON.stringify(enabled)) } catch {}
}

// Initialize from persisted preference
try {
  const v = JSON.parse(localStorage.getItem('retrievalAssist') || 'false')
  if (v) (axios.defaults.headers.common as any)[RETRIEVAL_HEADER] = 'true'
} catch {}

export const api = {
  async getDatasets() {
    const r = await axios.get('/api/datasets')
    return r.data.datasets
  },
  async getTables(datasetId: string) {
    const r = await axios.get(`/api/datasets/${encodeURIComponent(datasetId)}/tables`)
    return r.data.tables
  },
  async prepare(tables: {datasetId: string, tableId: string}[], sampleRows = 5) {
    const r = await axios.post('/api/prepare', { tables, sampleRows })
    return r.data
  },
  async generateKpis(tables: {datasetId: string, tableId: string}[], k = 5, prefer_cross: boolean = false) {
    const r = await axios.post('/api/generate_kpis', { tables, k, prefer_cross })
    return r.data.kpis
  },
  async runKpi(sql: string, filters?: any, date_column?: string, expected_schema?: string, opts?: { preview_limit?: number, validate_shape?: boolean }) {
    const payload: any = { sql, filters, date_column, expected_schema }
    if (opts && typeof opts.preview_limit === 'number') payload.preview_limit = opts.preview_limit
    if (opts && typeof opts.validate_shape === 'boolean') payload.validate_shape = opts.validate_shape
    const r = await axios.post('/api/run_kpi', payload)
    return r.data.rows
  },
  async saveDashboard(payload: { id?: string, name: string, kpis: any[], layout?: any[], layouts?: any, selected_tables: any[], global_filters?: any, theme?: any, tabs?: any[], tab_layouts?: Record<string, any[]>, last_active_tab?: string, version?: string }) {
    const r = await axios.post('/api/dashboards', payload)
    return r.data
  },
  async listDashboards() {
    const r = await axios.get('/api/dashboards')
    return r.data.dashboards
  },
  async getDashboard(id: string) {
    const r = await axios.get(`/api/dashboards/${encodeURIComponent(id)}`)
    return r.data
  },
  async deleteDashboard(id: string) {
    const r = await axios.delete(`/api/dashboards/${encodeURIComponent(id)}`)
    return r.data
  },
  async addToKpiCatalog(datasetId: string, tableId: string, kpis: any[]) {
    const r = await axios.post('/api/kpi_catalog', { datasetId, tableId, kpis })
    return r.data
  },
  async listKpiCatalog(params?: { datasetId?: string, tableId?: string }) {
    const r = await axios.get('/api/kpi_catalog', { params })
    return r.data.items
  },
  async editKpi(kpi: any, instruction: string) {
    const r = await axios.post('/api/kpi/edit', { kpi, instruction })
    return r.data as { kpi: any; markdown: string }
  },
  async editKpiChat(kpi: any, message: string, history?: { role: string; content: string }[], context?: any) {
    const r = await axios.post('/api/kpi/edit_chat', { kpi, message, history, context })
    return r.data as { reply: string; kpi?: any }
  },

  async acceptAiEditExample(payload: { intent: string; sql_before: string; sql_after: string; task_type?: string; dialect?: string; rationale?: string; kpi_before?: any; kpi_after?: any; tables_used?: string[] }) {
    const r = await axios.post('/api/ai_edit/accept_example', payload)
    return r.data as { status: 'ok' }
  },

  async generateCustomKpi(tables: {datasetId: string, tableId: string}[], description: string, clarifyingQuestions?: string[], answers?: string[]) {
    const r = await axios.post('/api/generate_custom_kpi', { tables, description, clarifying_questions: clarifyingQuestions, answers })
    return r.data
  },

  async getMostRecentDashboard() {
    const r = await axios.get('/api/dashboards/most-recent')
    return r.data.id as string | null
  },
  async cxoStart(dashboard_id: string, dashboard_name: string, active_tab: string) {
    const r = await axios.post('/api/cxo/start', { dashboard_id, dashboard_name, active_tab })
    return r.data.conversation_id as string
  },
  async cxoSend(conversation_id: string, message: string, context: any) {
    const r = await axios.post('/api/cxo/send', { conversation_id, message, context })
    return r.data.reply as string
  },

  async analystChat(message: string, kpis: any[], tables: {datasetId: string, tableId: string}[], history?: {role: string; content: string}[], prefer_cross: boolean = true) {
    const r = await axios.post('/api/analyst/chat', { message, kpis, tables, history, prefer_cross })
    return r.data as { reply: string; kpis?: any[] }
  },

  async getLineage(sql: string, dialect: 'bigquery' = 'bigquery') {
    const r = await axios.post('/api/lineage', { sql, dialect })
    return r.data as any
  }
}