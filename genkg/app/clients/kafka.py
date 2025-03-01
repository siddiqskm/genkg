import json
import logging
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError

from genkg.app.core.config import Settings

logger = logging.getLogger(__name__)

class KafkaDirectProducer:
    def __init__(self, settings: Settings):
        """
        Initialize Kafka direct producer client
        
        :param settings: Application settings with Kafka connection details
        """
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS.split(',')
        self.topic = settings.KAFKA_TOPIC_INGESTION
        
        # Initialize the KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Configure additional producer settings as needed
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,   # Retry failed sends
            request_timeout_ms=30000  # 30 seconds timeout
        )
        
        logger.info(f"Initialized Kafka producer with bootstrap servers: {self.bootstrap_servers}")

    def publish(self, message: Dict[str, Any]):
        """
        Publish a single message to a Kafka topic
        
        :param message: Message dictionary to publish
        """
        try:
            # Log the message for debugging
            logger.info(f"Publishing message to topic {self.topic}: {json.dumps(message)}")
            
            # Send the message
            future = self.producer.send(
                topic=self.topic,
                value=message
            )
            
            # Optional: Wait for the send to complete and get metadata
            metadata = future.get(timeout=10)
            
            logger.info(f"Message published to {metadata.topic}, partition {metadata.partition}, offset {metadata.offset}")
            
            return {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset
            }
            
        except KafkaError as e:
            logger.error(f"Failed to publish to Kafka: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error publishing to Kafka: {str(e)}")
            raise

    def close(self):
        """
        Close the Kafka producer
        """
        if self.producer:
            self.producer.flush()  # Make sure all messages are sent
            self.producer.close()
            logger.info("Kafka producer closed")