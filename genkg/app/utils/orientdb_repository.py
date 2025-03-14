import logging
from typing import List, Dict, Any
from fastapi import HTTPException

from app.core.config import Settings
from genkg.app.clients.orientdb import OrientDBRestClient

logger = logging.getLogger(__name__)

class OrientDBRepository:
    """Repository for OrientDB operations"""
    @staticmethod
    async def get_similar_user_recommendations(user_id: str, limit: int, settings: Settings) -> List[Dict[str, Any]]:
        """
        Get recommendations for a user based on similar users' preferences
        
        :param user_id: User ID
        :param limit: Maximum number of recommendations
        :param settings: Application settings
        :return: List of recommended items
        """
        client = None
        try:
            # Initialize OrientDB client
            client = OrientDBRestClient(settings)
            # Convert user_id to OrientDB record ID if needed
            user_rid = f"#{user_id}" if ":" in user_id else f"#66:0"
            # Execute recommendation query
            query = f"""
            SELECT FROM movies
            WHERE @rid IN (
              SELECT EXPAND(out('rated')) FROM (
                SELECT FROM users WHERE @rid <> {user_rid}
              )
            )
            AND @rid NOT IN (
              SELECT EXPAND(out('rated')) FROM {user_rid}
            )
            LIMIT {limit}
            """
            result = client.execute_command(query)
            return result.get('result', [])
        except Exception as e:
            logger.error(f"Failed to get similar user recommendations: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get recommendations: {str(e)}"
            )
        finally:
            if client:
                client.close()

    @staticmethod
    async def get_path_recommendations(user_id: str, limit: int, max_depth: int, settings: Settings) -> List[Dict[str, Any]]:
        """
        Get path-based recommendations for a user
        
        :param user_id: User ID
        :param limit: Maximum number of recommendations
        :param max_depth: Maximum path depth for traversal
        :param settings: Application settings
        :return: List of recommended items with paths
        """
        client = None
        try:
            # Initialize OrientDB client
            client = OrientDBRestClient(settings)
            # Convert user_id to OrientDB record ID if needed
            user_rid = f"#{user_id}" if ":" in user_id else f"#66:0"
            # Execute path recommendation query
            query = f"""
            SELECT
              @rid AS vertex_id,
              @class AS vertex_type,
              count(*) AS score,
              $path AS path
            FROM (
              TRAVERSE out('rated'), in('rated'), out('rated') FROM {user_rid}
              MAXDEPTH {max_depth}
              STRATEGY BREADTH_FIRST
            )
            WHERE @class = 'movies'
            AND @rid NOT IN (
              SELECT EXPAND(out('rated')) FROM {user_rid}
            )
            GROUP BY vertex_id
            ORDER BY score DESC
            LIMIT {limit}
            """
            result = client.execute_command(query)
            return result.get('result', [])
        except Exception as e:
            logger.error(f"Failed to get path recommendations: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get path recommendations: {str(e)}"
            )
        finally:
            if client:
                client.close()

    @staticmethod
    async def query_class(kg_id: str, class_name: str, limit: int, offset: int, settings: Settings) -> Dict[str, Any]:
        """
        Query vertices or edges by class name
        :param kg_id: Knowledge graph ID
        :param class_name: OrientDB class name to query
        :param limit: Maximum number of results
        :param offset: Offset for pagination
        :param settings: Application settings
        :return: Dictionary with results and count
        """
        client = None
        try:
            # Initialize OrientDB client
            client = OrientDBRestClient(settings)
            # Execute query to get records
            query = f"SELECT FROM {class_name} LIMIT {limit} SKIP {offset}"
            result = client.execute_command(query)
            # Get total count
            count_query = f"SELECT COUNT(*) as count FROM {class_name}"
            count_result = client.execute_command(count_query)
            # Extract count from result
            total_count = 0
            if count_result and 'result' in count_result and len(count_result['result']) > 0:
                total_count = count_result['result'][0].get('count', 0)
            return {
                "results": result.get('result', []),
                "total_count": total_count
            }
        except Exception as e:
            logger.error(f"Failed to query class {class_name}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to query class {class_name}: {str(e)}"
            )
        finally:
            if client:
                client.close()