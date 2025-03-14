from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, validator


class KGCreateResponse(BaseModel):
    kg_id: str
    status: str


class KGStatusResponse(BaseModel):
    kg_id: str
    status: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    processing_started: Optional[datetime] = None
    error: Optional[str] = None
    vertices_count: Optional[int] = 0
    edges_count: Optional[int] = 0
    source_type: Optional[str] = None
    source_path: Optional[str] = None

    # Just handle tuples that might come from Redis parsing
    @validator('created_at', 'updated_at', 'processing_started')
    def validate_datetime(cls, v):
        if isinstance(v, tuple) and len(v) > 0 and isinstance(v[0], datetime):
            return v[0]
        return v


class VertexMetadata(BaseModel):
    name: str
    count: int
    properties: List[str]


class EdgeMetadata(BaseModel):
    name: str
    count: int
    from_vertex: str
    to_vertex: str
    properties: List[str]


class KGMetadataResponse(BaseModel):
    vertices: List[VertexMetadata]
    edges: List[EdgeMetadata]


class PathElement(BaseModel):
    vertex_id: str
    edge_type: str


class Recommendation(BaseModel):
    vertex_id: str
    vertex_type: str
    properties: Dict[str, Any]
    score: float
    path: List[PathElement]


class RecommendationResponse(BaseModel):
    recommendations: List[Recommendation]


class Vertex(BaseModel):
    id: str
    type: str
    properties: Dict[str, Any]


class Edge(BaseModel):
    type: str
    properties: Dict[str, Any]


class Path(BaseModel):
    length: int
    vertices: List[Vertex]
    edges: List[Edge]


class PathResponse(BaseModel):
    paths: List[Path]


class ConnectedEdge(BaseModel):
    type: str
    direction: str
    count: int


class QueryVertex(BaseModel):
    id: str
    properties: Dict[str, Any]
    connected_edges: List[ConnectedEdge]


class PropertyQueryResponse(BaseModel):
    vertices: List[QueryVertex]
    total_count: int


class ComponentStatus(BaseModel):
    orientdb: bool
    kafka: bool
    flink: bool


class HealthResponse(BaseModel):
    status: str  # healthy, degraded, unhealthy
    components: Dict[str, bool]
    errors: Optional[List[str]] = None
    timestamp: datetime


class KGDeleteResponse(BaseModel):
    kg_id: str
    status: str
    message: str