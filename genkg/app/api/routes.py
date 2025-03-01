from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
import uuid
import logging
from datetime import datetime

from app.models.request import (
    KGCreateRequest,
    RecommendationRequest,
    PathRequest,
    PropertyQueryRequest
)
from app.models.response import (
    KGCreateResponse,
    KGStatusResponse,
    KGMetadataResponse,
    RecommendationResponse,
    PathResponse,
    PropertyQueryResponse,
    HealthResponse
)
from app.core.config import get_settings
from genkg.app.utils.db_helpers import (
    create_schema_in_orientdb_if_needed
)
from genkg.app.utils.kafka_helpers import publish_ingestion_config

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/kg", response_model=KGCreateResponse)
async def create_knowledge_graph(
    request: KGCreateRequest,
    background_tasks: BackgroundTasks,
    settings = Depends(get_settings)
):
    """
    Create a new knowledge graph based on input configuration
    """
    try:
        kg_id = await create_schema_in_orientdb_if_needed(request, settings)
        
        # Publish ingestion config to Kafka as background task
        background_tasks.add_task(
            publish_ingestion_config,
            request,
            kg_id,
            settings
        )
        
        return KGCreateResponse(
            kg_id=kg_id,
            status="INITIATED"
        )

    except Exception as e:
        logger.error(f"Knowledge graph creation failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.get("/kg/{kg_id}", response_model=KGStatusResponse)
async def get_knowledge_graph(kg_id: str):
    """
    Get details about a specific knowledge graph
    """
    try:
        # Logic to retrieve KG details
        return {
            "kg_id": kg_id,
            "status": "PROCESSING",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "error": None
        }
    except Exception as e:
        logger.error(f"Error retrieving knowledge graph: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kg", response_model=list[KGStatusResponse])
async def list_knowledge_graphs():
    """
    List all knowledge graphs
    """
    try:
        # Logic to list all KGs
        return [
            {
                "kg_id": "example_kg_id",
                "status": "COMPLETED",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "error": None
            }
        ]
    except Exception as e:
        logger.error(f"Error listing knowledge graphs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kg/{kg_id}/status", response_model=KGStatusResponse)
async def get_kg_status(kg_id: str):
    """
    Get status of knowledge graph creation process
    """
    try:
        return {
            "kg_id": kg_id,
            "status": "PROCESSING",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "error": None
        }
    except Exception as e:
        logger.error(f"Error retrieving status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kg/{kg_id}/metadata", response_model=KGMetadataResponse)
async def get_kg_metadata(kg_id: str):
    """
    Get metadata about the knowledge graph structure
    """
    try:
        return {
            "vertices": [
                {
                    "name": "example_vertex",
                    "count": 100,
                    "properties": ["prop1", "prop2"]
                }
            ],
            "edges": [
                {
                    "name": "example_edge",
                    "count": 50,
                    "from_vertex": "vertex1",
                    "to_vertex": "vertex2",
                    "properties": ["prop1"]
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error retrieving metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kg/{kg_id}/recommendations", response_model=RecommendationResponse)
async def get_recommendations(kg_id: str, request: RecommendationRequest):
    """
    Get recommendations based on graph traversal
    """
    try:
        return {
            "recommendations": [
                {
                    "vertex_id": "v1",
                    "vertex_type": "movie",
                    "properties": {},
                    "score": 0.95,
                    "path": [
                        {
                            "vertex_id": "v2",
                            "edge_type": "rated"
                        }
                    ]
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kg/{kg_id}/path", response_model=PathResponse)
async def find_paths(kg_id: str, request: PathRequest):
    """
    Find paths between vertices in the graph
    """
    try:
        return {
            "paths": [
                {
                    "length": 2,
                    "vertices": [
                        {
                            "id": "v1",
                            "type": "user",
                            "properties": {}
                        }
                    ],
                    "edges": [
                        {
                            "type": "rated",
                            "properties": {}
                        }
                    ]
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error finding paths: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kg/{kg_id}/query", response_model=PropertyQueryResponse)
async def query_properties(kg_id: str, request: PropertyQueryRequest):
    """
    Query vertices based on property filters
    """
    try:
        return {
            "vertices": [
                {
                    "id": "v1",
                    "properties": {},
                    "connected_edges": [
                        {
                            "type": "rated",
                            "direction": "out",
                            "count": 5
                        }
                    ]
                }
            ],
            "total_count": 1
        }
    except Exception as e:
        logger.error(f"Error querying properties: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/kg/{kg_id}")
async def delete_knowledge_graph(kg_id: str):
    """
    Delete a knowledge graph
    """
    try:
        # Logic to delete KG
        return {"message": f"Knowledge graph {kg_id} deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting knowledge graph: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Service health check endpoint
    """
    try:
        return {
            "status": "healthy",
            "components": {
                "orientdb": True,
                "kafka": True,
                "flink": True
            },
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        }