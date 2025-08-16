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
  async runKpi(sql: string) {
    const r = await axios.post('/api/run_kpi', { sql })
    return r.data.rows
  }
}