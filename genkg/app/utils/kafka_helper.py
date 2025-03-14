from datetime import datetime
import logging
from typing import Optional, Tuple
from genkg.app.clients.kafka import KafkaDirectProducer
from genkg.app.core.config import Settings
from genkg.app.models.request import KGCreateRequest

logger = logging.getLogger(__name__)


async def check_kafka_connection(settings: Settings) -> Tuple[bool, Optional[str]]:
    """
    Check Kafka connection health using KafkaDirectProducer

    :param settings: Application settings containing Kafka configuration
    :return: Tuple of (is_healthy, error_message)
    """
    client = None
    try:
        client = KafkaDirectProducer(settings)
        # Just test connection by sending a small health check message
        test_message = {
            "type": "health_check",
            "timestamp": datetime.now().isoformat()
        }
        client.publish(test_message)
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        if client:
            client.close()


async def publish_ingestion_config(request: KGCreateRequest, kg_id: str, settings: Settings):
    """
    Publish messages to Kafka topic
    
    :param messages: List of messages to publish
    :param settings: Application settings
    """
    client = None
    try:
        client = KafkaDirectProducer(settings)
        ingestion_config = {
            "kg_id": kg_id,
            "source": request.source.model_dump(),
            "vertices": [v.model_dump() for v in request.vertices],
            "edges": [e.model_dump() for e in request.edges],
        }
        return client.publish(ingestion_config)
    except Exception as e:
        logger.error(f"Kafka publishing failed: {str(e)}")
        raise
    finally:
        if client:
            client.close()