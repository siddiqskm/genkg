import json
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query
import logging
from datetime import datetime

from app.models.request import (
    KGCreateRequest,
    SimpleQueryRequest
)
from app.models.response import (
    KGCreateResponse,
    KGDeleteResponse,
    KGStatusResponse,
    PathRecommendationResponse,
    HealthResponse,
    SimpleQueryResponse,
    UserSimilarityRecommendationResponse
)
from app.core.config import get_redis_client, get_settings
from genkg.app.utils.flink_helper import check_flink_connection
from genkg.app.utils.misc import calculate_similarity_score, parse_iso_datetime
from genkg.app.utils.orientdb_helper import (
    check_orientdb_connection,
    cleanup_orientdb_resources,
    create_schema_in_orientdb_if_needed
)
from genkg.app.utils.kafka_helper import check_kafka_connection, publish_ingestion_config
from genkg.app.utils.orientdb_repository import OrientDBRepository

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/kg", response_model=KGCreateResponse)
async def create_knowledge_graph(
    request: KGCreateRequest,
    background_tasks: BackgroundTasks,
    settings = Depends(get_settings),
    redis_client = Depends(get_redis_client)
):
    """
    Create a new knowledge graph based on input configuration
    """
    try:
        kg_id = await create_schema_in_orientdb_if_needed(request, settings)
        
        # Store job status in Redis
        job_key = f"job:{kg_id}"
        job_status = {
            "status": "INITIATED",
            "created_at": datetime.now().isoformat(),
            "request": json.dumps(request.dict())
        }
        redis_client.hset(job_key, mapping=job_status)

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
async def get_knowledge_graph(
    kg_id: str,
    redis_client = Depends(get_redis_client)
):
    """
    Get details about a specific knowledge graph
    """
    try:
        # Retrieve KG details from Redis
        job_key = f"job:{kg_id}"
        job_data = redis_client.hgetall(job_key)

        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Knowledge graph with ID {kg_id} not found"
            )

        # Parse stored request if needed
        request_data = {}
        if "request" in job_data:
            request_data = json.loads(job_data["request"])

        # Build response
        response = {
            "kg_id": kg_id,
            "status": job_data.get("status", "UNKNOWN"),
            "created_at": parse_iso_datetime(job_data.get("created_at")),
            "updated_at": parse_iso_datetime(job_data.get("updated_at")),
            "processing_started": parse_iso_datetime(job_data.get("processing_started")),
            "error": job_data.get("error"),
            "vertices_count": len(request_data.get("vertices", [])) if request_data else 0,
            "edges_count": len(request_data.get("edges", [])) if request_data else 0,
            "source_type": request_data.get("source", {}).get("type") if request_data else None,
            "source_path": request_data.get("source", {}).get("path") if request_data else None
        }

        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving knowledge graph: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kg", response_model=list[KGStatusResponse])
async def list_knowledge_graphs(
    redis_client = Depends(get_redis_client),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """
    List all knowledge graphs with pagination
    """
    try:
        # Get all job keys from Redis
        job_keys = redis_client.keys("job:*")

        # Apply pagination
        paginated_keys = job_keys[offset:offset + limit]

        # Fetch data for each KG
        result = []
        for job_key in paginated_keys:
            kg_id = job_key.split(":", 1)[1]  # Extract KG ID from key
            job_data = redis_client.hgetall(job_key)

            if job_data:
                # Parse request data if needed
                request_data = {}
                if "request" in job_data:
                    request_data = json.loads(job_data["request"])
                # Build response (matching structure from get_knowledge_graph)
                kg_status = {
                    "kg_id": kg_id,
                    "status": job_data.get("status", "UNKNOWN"),
                    "created_at": parse_iso_datetime(job_data.get("created_at")),
                    "updated_at": parse_iso_datetime(job_data.get("updated_at")),
                    "processing_started": parse_iso_datetime(job_data.get("processing_started")),
                    "error": job_data.get("error"),
                    "vertices_count": len(request_data.get("vertices", [])) if request_data else 0,
                    "edges_count": len(request_data.get("edges", [])) if request_data else 0,
                    "source_type": request_data.get("source", {}).get("type") if request_data else None,
                    "source_path": request_data.get("source", {}).get("path") if request_data else None
                }
                result.append(kg_status)
        return result
    except Exception as e:
        logger.error(f"Error listing knowledge graphs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/similar-to-user", response_model=UserSimilarityRecommendationResponse)
async def get_similar_to_user_recommendations(
    kg_id: str = Query(..., description="Knowledge graph ID"),
    user_id: str = Query(..., description="User ID for whom to generate recommendations"),
    limit: int = Query(10, description="Number of recommendations to return"),
    settings = Depends(get_settings)
):
    """
    Get recommendations based on similar users' preferences
    """
    try:
        # Get recommendations from repository
        recommendations = await OrientDBRepository.get_similar_user_recommendations(
            user_id=user_id,
            limit=limit,
            settings=settings
        )
        # Format the recommendations according to required structure
        formatted_recommendations = []
        for rec in recommendations:
            # Extract metadata fields
            metadata = {
                "@rid": rec.get("@rid", ""),
                "@version": rec.get("@version", 0),
                "@class": rec.get("@class", ""),
                "@fieldTypes": rec.get("@fieldTypes", "")
            }
            # Dynamically extract property fields
            properties = {}
            in_data = {}
            out_data = {}
            # Process all fields in the record
            for key, value in rec.items():
                # Skip metadata fields
                if key.startswith('@'):
                    continue
                # Categorize fields based on prefix
                if key.startswith('in_'):
                    in_data[key] = value
                elif key.startswith('out_'):
                    out_data[key] = value
                else:
                    # All other fields are properties
                    properties[key] = value
            # Create recommendation item
            item = {
                "metadata": metadata,
                "properties": properties,
                "in_marked": in_data,
                "out_marked": out_data,
                "similarity_score": calculate_similarity_score(rec)
            }
            formatted_recommendations.append(item)
        # Create response
        return UserSimilarityRecommendationResponse(
            recommendations=formatted_recommendations,
            metadata={
                "total_count": len(formatted_recommendations),
                "user_id": user_id
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Similar-to-user recommendation failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.get("/recommendations/similar-to-user/path", response_model=PathRecommendationResponse)
async def get_path_recommendations(
    kg_id: str = Query(..., description="Knowledge graph ID"),
    user_id: str = Query(..., description="User ID for whom to generate recommendations"),
    limit: int = Query(10, description="Number of recommendations to return"),
    max_depth: int = Query(5, description="Maximum path depth for traversal"),
    settings = Depends(get_settings)
):
    """
    Get path-based recommendations based on user similarity navigation paths
    """
    try:
        # Get path-based recommendations from repository
        recommendations = await OrientDBRepository.get_path_recommendations(
            user_id=user_id,
            limit=limit,
            max_depth=max_depth,
            settings=settings
        )
        # Format the recommendations exactly as returned by OrientDB
        formatted_recommendations = []
        for rec in recommendations:
            formatted_recommendations.append({
                "vertex_id": rec.get("vertex_id", ""),
                "vertex_type": rec.get("vertex_type", ""),
                "score": rec.get("score", 0),
                "path": rec.get("path", [])
            })
        return PathRecommendationResponse(
            recommendations=formatted_recommendations,
            metadata={
                "total_count": len(formatted_recommendations),
                "user_id": user_id,
                "max_depth": max_depth
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Path-based recommendation failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.delete("/kg/{kg_id}", response_model=KGDeleteResponse)
async def delete_knowledge_graph(
    kg_id: str,
    redis_client = Depends(get_redis_client),
    settings = Depends(get_settings)
):
    """
    Delete a knowledge graph and clean up related resources
    """
    try:
        # Retrieve KG details from Redis
        job_key = f"job:{kg_id}"
        job_data = redis_client.hgetall(job_key)

        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Knowledge graph with ID {kg_id} not found"
            )

        # Parse request data to get source path and structure info
        request_data = {}
        if "request" in job_data:
            request_data = json.loads(job_data["request"])

        # Step 1: Clean up OrientDB resources
        try:
            await cleanup_orientdb_resources(kg_id, request_data, settings)
        except Exception as e:
            logger.error(f"Error cleaning up OrientDB resources: {str(e)}")
            # Continue with Redis cleanup even if OrientDB cleanup fails

        # Step 2: Clean up Redis data
        redis_client.delete(job_key)

        # Step 3: Optional - Add additional cleanup like Kafka messages or job cancellation
        return KGDeleteResponse(
            kg_id=kg_id,
            status="DELETED",
            message="Knowledge graph and related resources successfully deleted"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting knowledge graph: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/kg/{kg_id}/query", response_model=SimpleQueryResponse)
async def query_class(
    kg_id: str,
    request: SimpleQueryRequest,
    settings = Depends(get_settings)
):
    """
    Query vertices or edges by class name with pagination
    """
    try:
        result = await OrientDBRepository.query_class(
            kg_id=kg_id,
            class_name=request.class_name,
            limit=request.limit,
            offset=request.offset,
            settings=settings
        )
        return SimpleQueryResponse(
            results=result["results"],
            total_count=result["total_count"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error querying class: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=HealthResponse)
async def health_check(
    settings = Depends(get_settings),
    redis_client = Depends(get_redis_client)
):
    """
    Service health check endpoint that actually checks component status
    """
    components = {
        "orientdb": False,
        "kafka": False,
        "redis": False,
        "flink": False
    }
    error_messages = []
    # Check OrientDB
    orientdb_healthy, orientdb_error = await check_orientdb_connection(settings)
    components["orientdb"] = orientdb_healthy
    if not orientdb_healthy and orientdb_error:
        error_messages.append(f"OrientDB: {orientdb_error}")
    # Check Redis (already have a client)
    try:
        redis_client.ping()
        components["redis"] = True
    except Exception as e:
        error_messages.append(f"Redis: {str(e)}")
    # Check Kafka
    kafka_healthy, kafka_error = await check_kafka_connection(settings)
    components["kafka"] = kafka_healthy
    if not kafka_healthy and kafka_error:
        error_messages.append(f"Kafka: {kafka_error}")
    # Check Flink (through REST API if available)
    flink_healthy, flink_error = await check_flink_connection(settings)
    components["flink"] = flink_healthy
    if not flink_healthy and flink_error:
        error_messages.append(f"Flink: {flink_error}")
    # Determine overall status
    all_healthy = all(components.values())
    if all_healthy:
        return {
            "status": "healthy",
            "components": components,
            "timestamp": datetime.utcnow()
        }
    else:
        return {
            "status": "degraded" if any(components.values()) else "unhealthy",
            "components": components,
            "errors": error_messages,
            "timestamp": datetime.utcnow()
        }