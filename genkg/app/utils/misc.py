from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def parse_iso_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO datetime string safely, handling different formats including Z timezone
    
    :param date_str: ISO format datetime string
    :return: datetime object or None if input is None/invalid
    """
    logger.debug(f"Parsing ISO datetime: {date_str}")

    if not date_str:
        return None
        
    try:
        # Handle Z timezone
        if date_str.endswith('Z'):
            date_str = date_str[:-1]
        return datetime.fromisoformat(date_str)
    except ValueError:
        # Fallback to now if parsing fails
        logger.error("Issue converting ISO datetime string: {date_str}")
        return None