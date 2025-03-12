from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    type: str = "local_files"  # For now only local_files
    access_credentials: Optional[Dict] = None
    path: str


class ColumnConfig(BaseModel):
    name: str
    type: str
    is_key: Optional[bool] = False


class VertexConfig(BaseModel):
    name: str
    file_pattern: str
    columns: List[ColumnConfig]
    distinct: Optional[bool] = False


class EdgePropertyConfig(BaseModel):
    name: str
    type: str


class EdgeMappingConfig(BaseModel):
    from_key: str
    to_key: str


class EdgeConfig(BaseModel):
    name: str
    from_vertex: str
    to_vertex: str
    file_pattern: str
    properties: List[EdgePropertyConfig]
    mapping: EdgeMappingConfig


class KGCreateRequest(BaseModel):
    source: SourceConfig
    vertices: List[VertexConfig]
    edges: List[EdgeConfig]


class PropertyFilter(BaseModel):
    property: str
    operator: str  # "eq", "gt", "lt", etc.
    value: Any


class RecommendationRequest(BaseModel):
    from_vertex: str
    to_vertex: str
    vertex_id: str
    max_depth: Optional[int] = 3
    limit: Optional[int] = 10
    filters: Optional[PropertyFilter] = None


class PathRequest(BaseModel):
    from_vertex_type: str
    from_vertex_id: str
    to_vertex_type: str
    to_vertex_id: str
    max_depth: Optional[int] = 3


class PropertyQueryRequest(BaseModel):
    vertex_type: str
    filters: List[PropertyFilter]
    limit: Optional[int] = 10
    offset: Optional[int] = 0
