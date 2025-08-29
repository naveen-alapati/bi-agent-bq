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
	prefer_cross: Optional[bool] = False
	thought_graph_id: Optional[str] = None
	thought_graph: Optional[Dict[str, Any]] = None


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
	preview_limit: Optional[int] = 0
	validate_shape: Optional[bool] = False


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


class AnalystChatMessage(BaseModel):
	role: str
	content: str


class AnalystChatRequest(BaseModel):
	message: str
	kpis: List[KPIItem]
	tables: List[TableRef]
	prefer_cross: Optional[bool] = False
	history: Optional[List[AnalystChatMessage]] = None


class AnalystChatResponse(BaseModel):
	reply: str
	kpis: Optional[List[KPIItem]] = None


# Thought Graphs
class ThoughtGraphNode(BaseModel):
	id: str
	type: str
	label: Optional[str] = None


class ThoughtGraphEdge(BaseModel):
	source: str
	target: str
	type: str


class ThoughtGraphJoinPair(BaseModel):
	left: str
	right: str


class ThoughtGraphJoin(BaseModel):
	id: Optional[str] = None
	left_table: Optional[str] = None
	right_table: Optional[str] = None
	type: Optional[str] = None
	on: Optional[str] = None
	pairs: Optional[List[ThoughtGraphJoinPair]] = None


class ThoughtGraphPayload(BaseModel):
	graph: Dict[str, Any]
	joins: Optional[List[ThoughtGraphJoin]] = None


class ThoughtGraphSaveRequest(BaseModel):
	id: Optional[str] = None
	name: str
	primary_dataset_id: Optional[str] = None
	datasets: Optional[List[str]] = None
	selected_tables: List[TableRef]
	graph: Dict[str, Any]


class ThoughtGraphSaveResponse(BaseModel):
	id: str
	name: str
	version: str


class ThoughtGraphListItem(BaseModel):
	id: str
	name: str
	version: Optional[str] = None
	primary_dataset_id: Optional[str] = None
	datasets: Optional[List[str]] = None
	created_at: Optional[str] = None
	updated_at: Optional[str] = None


class ThoughtGraphListResponse(BaseModel):
	graphs: List[ThoughtGraphListItem]


class ThoughtGraphGetResponse(BaseModel):
	id: str
	name: str
	version: Optional[str] = None
	primary_dataset_id: Optional[str] = None
	datasets: Optional[List[str]] = None
	selected_tables: List[TableRef]
	graph: Dict[str, Any]
	created_at: Optional[str] = None
	updated_at: Optional[str] = None


class ThoughtGraphGenerateRequest(BaseModel):
	tables: List[TableRef]
	datasets: Optional[List[str]] = None
	name: Optional[str] = None
	prompt: Optional[str] = None


class ThoughtGraphGenerateResponse(BaseModel):
	graph: Dict[str, Any]
	name: str