export type TableRef = { datasetId: string; tableId: string }
export type KPI = {
  id: string
  name: string
  short_description: string
  chart_type: 'line' | 'bar' | 'pie' | 'area' | 'scatter'
  d3_chart: string
  expected_schema: string
  sql: string
}