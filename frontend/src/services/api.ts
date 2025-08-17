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
  async saveDashboard(payload: { id?: string, name: string, kpis: any[], layout?: any[], layouts?: any, selected_tables: any[], global_filters?: any }) {
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
  async addToKpiCatalog(datasetId: string, tableId: string, kpis: any[]) {
    const r = await axios.post('/api/kpi_catalog', { datasetId, tableId, kpis })
    return r.data
  },
  async listKpiCatalog(params?: { datasetId?: string, tableId?: string }) {
    const r = await axios.get('/api/kpi_catalog', { params })
    return r.data.items
  }
}