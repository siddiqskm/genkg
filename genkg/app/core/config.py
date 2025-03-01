from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache
import logging
import sys
import os
from logging.handlers import RotatingFileHandler

class Settings(BaseSettings):
    # App Settings
    APP_NAME: str
    DEBUG: bool
    
    # API Settings
    API_V1_STR: str
    
    # External Services URLs
    ORIENTDB_URL: str
    ORIENTDB_DB: str
    ORIENTDB_USER: str
    ORIENTDB_PASSWORD: str
    
    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC_INGESTION: str
    
    # Logging
    LOG_LEVEL: str
    LOG_FILE_PATH: str
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB
    LOG_FILE_COUNT: int = 5

    class Config:
        env_file = ".env"
        case_sensitive = True

    def setup_logging(self) -> None:
        """Configure application-wide logging"""
        # Get the root logger
        root_logger = logging.getLogger()
        
        # Set the level
        level = getattr(logging, self.LOG_LEVEL)
        root_logger.setLevel(level)
        
        # Check if root logger already has handlers (don't add duplicates)
        has_console_handler = any(isinstance(h, logging.StreamHandler) and 
                                getattr(h, 'stream', None) == sys.stdout 
                                for h in root_logger.handlers)
        
        # Create formatter
        formatter = logging.Formatter(self.LOG_FORMAT)
        
        if not has_console_handler:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(level)
            root_logger.addHandler(console_handler)
        
        # Try to set up file logging
        try:
            # Check for existing file handlers
            has_file_handler = any(isinstance(h, logging.FileHandler) and 
                                  getattr(h, 'baseFilename', None) == self.LOG_FILE_PATH
                                  for h in root_logger.handlers)
            
            if not has_file_handler:
                os.makedirs(os.path.dirname(self.LOG_FILE_PATH), exist_ok=True)
                file_handler = RotatingFileHandler(
                    self.LOG_FILE_PATH,
                    maxBytes=self.LOG_FILE_SIZE,
                    backupCount=self.LOG_FILE_COUNT
                )
                file_handler.setFormatter(formatter)
                file_handler.setLevel(level)
                root_logger.addHandler(file_handler)
        except Exception as e:
            logging.warning(f"Could not set up file logging: {str(e)}")
        
        # Test logging
        logger = logging.getLogger(__name__)
        logger.debug("DEBUG Enabled")
        logger.info("INFO Enabled")

@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance
    """
    return Settings()