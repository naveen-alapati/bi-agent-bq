from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class TableRef(BaseModel):
	datasetId: str
	tableId: str


class DatasetItem(BaseModel):
	datasetId: str
	friendlyName: Optional[str] = None
	description: Optional[str] = None
	isBackendCreated: Optional[bool] = False


class DatasetResponse(BaseModel):
	datasets: List[DatasetItem]


class TableInfoItem(BaseModel):
	tableId: str
	rowCount: Optional[int] = None
	created: Optional[str] = None
	lastModified: Optional[str] = None


class TableInfoResponse(BaseModel):
	dataset_id: str
	tables: List[TableInfoItem]


class PrepareRequest(BaseModel):
	tables: List[TableRef]
	sampleRows: Optional[int] = None


class PrepareResponse(BaseModel):
	status: str
	prepared: Any


class GenerateKpisRequest(BaseModel):
	tables: List[TableRef]
	k: Optional[int] = None


class GenerateKpisResponse(BaseModel):
	kpis: List[Dict[str, Any]]


class RunKpiRequest(BaseModel):
	sql: str
	filters: Optional[Dict[str, Any]] = None
	date_column: Optional[str] = None
	expected_schema: Optional[str] = None


class RunKpiResponse(BaseModel):
	rows: List[Dict[str, Any]]


class DashboardSaveRequest(BaseModel):
	id: Optional[str] = None
	name: str
	kpis: List[Dict[str, Any]]
	layout: Optional[List[Dict[str, Any]]] = None
	layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	selected_tables: List[TableRef]
	global_filters: Optional[Dict[str, Any]] = None
	theme: Optional[Dict[str, Any]] = None
	version: Optional[str] = None
	tabs: Optional[List[Dict[str, Any]]] = None
	tab_layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	last_active_tab: Optional[str] = None


class DashboardSaveResponse(BaseModel):
	id: str
	name: str
	version: str


class DashboardListResponse(BaseModel):
	dashboards: List[Dict[str, Any]]


class DashboardGetResponse(BaseModel):
	id: str
	name: str
	kpis: List[Dict[str, Any]]
	layout: Optional[List[Dict[str, Any]]] = None
	layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	selected_tables: List[Dict[str, Any]]
	global_filters: Optional[Dict[str, Any]] = None
	theme: Optional[Dict[str, Any]] = None
	version: Optional[str] = None
	tabs: Optional[List[Dict[str, Any]]] = None
	tab_layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
	last_active_tab: Optional[str] = None
	created_at: Optional[str] = None
	updated_at: Optional[str] = None


class KPICatalogAddRequest(BaseModel):
	datasetId: str
	tableId: str
	kpis: List[Dict[str, Any]]


class KPICatalogListResponse(BaseModel):
	items: List[Dict[str, Any]]
