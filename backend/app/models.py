from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class DatasetInfo(BaseModel):
    datasetId: str
    friendlyName: Optional[str] = None
    description: Optional[str] = None


class DatasetResponse(BaseModel):
    datasets: List[DatasetInfo]


class TableInfo(BaseModel):
    tableId: str
    rowCount: Optional[int] = None
    created: Optional[str] = None
    lastModified: Optional[str] = None


class TableInfoResponse(BaseModel):
    dataset_id: str
    tables: List[TableInfo]


class TableRef(BaseModel):
    datasetId: str
    tableId: str


class PrepareRequest(BaseModel):
    tables: List[TableRef]
    sampleRows: Optional[int] = 5


class PreparedTable(BaseModel):
    datasetId: str
    tableId: str
    embed_rows: int


class PrepareResponse(BaseModel):
    status: str
    prepared: List[PreparedTable]


class GenerateKpisRequest(BaseModel):
    tables: List[TableRef]
    k: Optional[int] = 5


class KPIItem(BaseModel):
    id: str
    name: str
    short_description: str
    chart_type: str
    d3_chart: str
    expected_schema: str
    sql: str
    engine: Optional[str] = None
    vega_lite_spec: Optional[Dict[str, Any]] = None


class GenerateKpisResponse(BaseModel):
    kpis: List[KPIItem]


class RunKpiRequest(BaseModel):
    sql: str


class RunKpiResponse(BaseModel):
    rows: List[Dict[str, Any]]


# Dashboards
class DashboardSaveRequest(BaseModel):
    id: Optional[str] = None
    name: str
    kpis: List[KPIItem]
    layout: List[Dict[str, Any]]
    selected_tables: List[TableRef]


class DashboardSaveResponse(BaseModel):
    id: str
    name: str


class DashboardSummary(BaseModel):
    id: str
    name: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DashboardListResponse(BaseModel):
    dashboards: List[DashboardSummary]


class DashboardGetResponse(BaseModel):
    id: str
    name: str
    kpis: List[KPIItem]
    layout: List[Dict[str, Any]]
    selected_tables: List[TableRef]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None