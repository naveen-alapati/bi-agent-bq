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
	filter_date_column: Optional[str] = None
	tabs: Optional[List[str]] = None


class GenerateKpisResponse(BaseModel):
	kpis: List[KPIItem]


class RunKpiRequest(BaseModel):
	sql: str
	filters: Optional[Dict[str, Any]] = None
	date_column: Optional[str] = None
	expected_schema: Optional[str] = None


class RunKpiResponse(BaseModel):
	rows: List[Dict[str, Any]]


# KPI Catalog
class KPICatalogAddRequest(BaseModel):
	datasetId: str
	tableId: str
	kpis: List[KPIItem]


class KPICatalogItem(BaseModel):
	id: str
	name: str
	sql: str
	chart_type: str
	expected_schema: str
	dataset_id: str
	table_id: str
	tags: Optional[Dict[str, Any]] = None
	engine: Optional[str] = None
	vega_lite_spec: Optional[Dict[str, Any]] = None


class KPICatalogListResponse(BaseModel):
	items: List[KPICatalogItem]


# Tabs
class DashboardTab(BaseModel):
	id: str
	name: str
	order: int


# Dashboards
class DashboardSaveRequest(BaseModel):
	id: Optional[str] = None
	name: str
	version: Optional[str] = None
	kpis: List[KPIItem]
	layout: Optional[List[Dict[str, Any]]] = None
	layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	selected_tables: List[TableRef]
	global_filters: Optional[Dict[str, Any]] = None
	theme: Optional[Dict[str, Any]] = None
	# New
	tabs: Optional[List[DashboardTab]] = None
	tab_layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	last_active_tab: Optional[str] = None


class DashboardSaveResponse(BaseModel):
	id: str
	name: str
	version: str


class DashboardSummary(BaseModel):
	id: str
	name: str
	version: Optional[str] = None
	created_at: Optional[str] = None
	updated_at: Optional[str] = None


class DashboardListResponse(BaseModel):
	dashboards: List[DashboardSummary]


class DashboardGetResponse(BaseModel):
	id: str
	name: str
	version: Optional[str] = None
	kpis: List[KPIItem]
	layout: Optional[List[Dict[str, Any]]] = None
	layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	selected_tables: List[TableRef]
	global_filters: Optional[Dict[str, Any]] = None
	theme: Optional[Dict[str, Any]] = None
	created_at: Optional[str] = None
	updated_at: Optional[str] = None
	# New
	tabs: Optional[List[DashboardTab]] = None
	tab_layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	last_active_tab: Optional[str] = None