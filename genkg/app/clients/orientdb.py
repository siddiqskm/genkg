import httpx
import json
import logging
import urllib.parse
from typing import Dict, List, Any
from app.core.config import Settings
from fastapi import HTTPException

logger = logging.getLogger(__name__)

class OrientDBRestClient:
    def __init__(self, settings: Settings):
        """
        Initialize OrientDB REST API client
        
        :param settings: Application settings with connection details
        """
        # Parse the URL to ensure we have the correct base URL
        parsed_url = urllib.parse.urlparse(settings.ORIENTDB_URL)
        self.base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        self.database = settings.ORIENTDB_DB
        self.username = settings.ORIENTDB_USER
        self.password = settings.ORIENTDB_PASSWORD
        
        # Create a base client with authentication
        self.client = httpx.Client(
            base_url=self.base_url,
            auth=(self.username, self.password),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )
    
    def _make_database_url(self, endpoint: str) -> str:
        """
        Construct full URL for database-specific operations
        
        :param endpoint: API endpoint
        :return: Full URL for the endpoint
        """
        return f"/command/{self.database}/{endpoint}"

    def create_vertex(self, vertex_class: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a vertex in the specified class
        
        :param vertex_class: Name of the vertex class
        :param properties: Dictionary of vertex properties
        :return: Created vertex details
        """
        try:
            url = self._make_database_url(f"document/{vertex_class}")
            response = self.client.post(
                url, 
                json=properties
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to create vertex: {str(e)}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
            raise
    
    def create_class(self, class_name: str, extends: str = 'V'):
        """
        Create a new vertex class
        
        :param class_name: Name of the class to create
        :param extends: Parent class (default is 'V' for vertex)
        """
        try:
            url = self._make_database_url("sql")
            command = f"CREATE CLASS {class_name} EXTENDS {extends}"
            
            response = self.client.post(
                url,
                json={"command": command}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to create class: {str(e)}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
            raise
    
    def execute_command(self, command: str) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL command
        
        :param command: SQL command to execute
        :return: Command results
        """
        logger.debug("Executing command in OrientDB: %s", command)
        try:
            url = self._make_database_url("sql")
            response = self.client.post(
                url,
                json={"command": command}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to execute command: {str(e)}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
            raise
    
    def close(self):
        """
        Close the HTTP client
        """
        self.client.close()