import logging
import uuid
from typing import List, Tuple

import httpx
from fastapi import HTTPException

from app.models.request import KGCreateRequest
from app.core.config import Settings
from genkg.app.clients.orientdb import OrientDBRestClient


logger = logging.getLogger(__name__)


async def generate_orientdb_schema_commands(request: KGCreateRequest) -> List[str]:
    """Generate OrientDB schema creation commands from request"""
    commands = []

    # Create vertex classes
    for vertex in request.vertices:
        commands.append(f"create class {vertex.name} extends v")

        # Create properties for vertex
        for column in vertex.columns:
            commands.append(
                f"create property {vertex.name}.{column.name} {column.type.lower()}"
            )

        # Create index if column is key
        for column in vertex.columns:
            if column.is_key:
                commands.append(
                    f"create index {vertex.name}.{column.name} on {vertex.name} ({column.name}) unique_hash_index"
                )

    # Create edge classes
    for edge in request.edges:
        commands.append(f"create class {edge.name} extends e")

        # Create properties for edge
        for prop in edge.properties:
            commands.append(
                f"create property {edge.name}.{prop.name} {prop.type.lower()}"
            )
    logger.debug(f"Schema commands generated: {commands}")
    return commands


async def setup_schema_mapper(settings: Settings):
    """
    Ensure the schema_mapper class exists in OrientDB
    
    :param settings: Application settings
    """
    client = None
    try:
        client = OrientDBRestClient(settings)
        _create_schema_mapper_if_needed(client)
    except Exception as e:
        logger.error(f"Failed to setup schema mapper: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to setup schema mapper: {str(e)}"
        )
    finally:
        if client:
            client.close()


def _create_schema_mapper_if_needed(client: OrientDBRestClient):
    """
    Create the SchemaMapper class if it doesn't exist
    
    :param client: OrientDB client instance
    """
    try:
        result = client.execute_command("SELECT FROM (SELECT EXPAND(classes) FROM metadata:schema) WHERE name = 'SchemaMapper'")
        if not result or len(result['result']) == 0:
            client.execute_command("CREATE CLASS SchemaMapper EXTENDS V")
            client.execute_command("CREATE PROPERTY SchemaMapper.kg_id STRING")
            client.execute_command("CREATE PROPERTY SchemaMapper.path STRING")
            client.execute_command("CREATE INDEX SchemaMapper.path_idx ON SchemaMapper(path) UNIQUE")
            logger.info("Created SchemaMapper class")
    except Exception as e:
        logger.error(f"Failed to check/create SchemaMapper class: {str(e)}")
        raise


async def create_schema_in_orientdb_if_needed(request: KGCreateRequest, settings: Settings) -> str:
    """
    Execute schema creation commands in OrientDB only if no schema exists for the path
    
    :param request: Knowledge graph creation request
    :param settings: Application settings
    :return: Knowledge graph ID (either new or existing)
    """
    client = None
    try:
        client = OrientDBRestClient(settings)
        
        # Check if schema creation is required
        creation_required, existing_kg_id = is_schema_creation_required(client, request.source.path)
        
        if creation_required:
            logger.info(f"Creating schema for path: {request.source.path}")
            
            # Generate a new kg_id
            new_kg_id = str(uuid.uuid4())
            
            # Generate OrientDB schema commands
            schema_commands = await generate_orientdb_schema_commands(request)
            logger.info("Schema commands generated are: %s", schema_commands)
            
            # Execute all schema creation commands
            for command in schema_commands:
                client.execute_command(command)
            
            # Record that we've created a schema for this path
            record_schema_creation(client, request.source.path, new_kg_id)
            logger.info(f"Schema creation for path {request.source.path} complete and recorded with kg_id {new_kg_id}")
            
            return new_kg_id
        else:
            logger.info(f"Schema for path {request.source.path} already exists with kg_id {existing_kg_id}, skipping")
            return existing_kg_id
    
    except Exception as e:
        logger.error(f"OrientDB schema check/creation failed: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create schema in OrientDB: {str(e)}"
        )
    finally:
        if client:
            client.close()


def is_schema_creation_required(client: OrientDBRestClient, file_path: str) -> Tuple[bool, str]:
    """
    Check if schema creation is required for a specific file path
    
    :param client: OrientDB client instance
    :param file_path: File path to check
    :return: Tuple of (creation_required, kg_id if exists)
    """
    try:
        command = f"SELECT FROM SchemaMapper WHERE path = '{file_path}'"
        result = client.execute_command(command)
        
        if len(result['result']) == 0:
            # Schema creation is required
            return True, ""
        else:
            # Schema exists, return the kg_id
            return False, result['result'][0]['kg_id']
    
    except Exception as e:
        logger.error(f"Failed to check schema requirements: {str(e)}")
        raise


def record_schema_creation(client: OrientDBRestClient, path: str, kg_id: str):
    """
    Record that a schema has been created for a path
    
    :param client: OrientDB client instance
    :param path: File path the schema was created for
    :param kg_id: Knowledge graph ID
    """
    try:
        command = f"CREATE VERTEX SchemaMapper SET path = '{path}', kg_id = '{kg_id}'"
        return client.execute_command(command)
    
    except Exception as e:
        logger.error(f"Failed to record schema creation: {str(e)}")
        raise