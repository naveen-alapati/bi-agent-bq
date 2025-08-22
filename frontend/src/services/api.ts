import axios from 'axios'

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
  async generateKpis(tables: {datasetId: string, tableId: string}[], k = 5) {
    const r = await axios.post('/api/generate_kpis', { tables, k })
    return r.data.kpis
  },
  async runKpi(sql: string, filters?: any, date_column?: string, expected_schema?: string) {
    const r = await axios.post('/api/run_kpi', { sql, filters, date_column, expected_schema })
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

  // KPI Draft flow
  async kpiDraftsGenerate(tables: {datasetId: string, tableId: string}[], k = 5) {
    const r = await axios.post('/api/kpi_drafts/generate', { tables, k })
    return r.data.kpis as any[]
  },
  async kpiDraftsValidate(tables: {datasetId: string, tableId: string}[], kpis: any[]) {
    const r = await axios.post('/api/kpi_drafts/validate', { tables, kpis })
    return r.data.results as { id: string; valid: boolean; issues: { type: string; message: string }[] }[]
  },
  async kpiDraftsFinalize(kpis: any[]) {
    const r = await axios.post('/api/kpi_drafts/finalize', { kpis })
    return r.data as { inserted: number }
  }
}