import urllib.error
from typing import Tuple, Optional
import aiohttp

from genkg.app.core.config import Settings


async def check_flink_connection(settings: Settings) -> Tuple[bool, Optional[str]]:
    """
    Check Flink connection health using the Flink REST API
    
    :param settings: Application settings containing Flink REST endpoint
    :return: Tuple of (is_healthy, error_message)
    """
    try:
        flink_url = f"{settings.FLINK_URL}/jobs/overview"
        
        # Use aiohttp for async HTTP requests
        async with aiohttp.ClientSession() as session:
            async with session.get(flink_url, timeout=5) as response:
                if response.status == 200:
                    return True, None
                else:
                    return False, f"HTTP {response.status}"
    except aiohttp.ClientConnectorError as e:
        return False, f"Connection error: {str(e)}"
    except aiohttp.ClientError as e:
        return False, f"Client error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"