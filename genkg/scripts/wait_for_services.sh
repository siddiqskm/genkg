#!/bin/bash
set -e

echo "Waiting for Kafka, OrientDB and Redis to be ready..."

# Check if environment variables are set
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "KAFKA_BOOTSTRAP_SERVERS is not set"
    exit 1
fi

if [ -z "$ORIENTDB_URL" ]; then
    echo "ORIENTDB_URL is not set"
    exit 1
fi

# Redis connection settings
REDIS_HOST=${REDIS_HOST:-redis}
REDIS_PORT=${REDIS_PORT:-6379}

# Check if REDIS_URL is set, and extract host/port from it
if [ ! -z "$REDIS_URL" ]; then
    # Format expected: redis://host:port
    REDIS_HOST=$(echo $REDIS_URL | sed -E 's|^redis://([^:]+).*|\1|')
    REDIS_PORT=$(echo $REDIS_URL | sed -E 's|^redis://[^:]+:([0-9]+).*|\1|')
fi

# Extract host and port from KAFKA_BOOTSTRAP_SERVERS
KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d',' -f1 | cut -d':' -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d',' -f1 | cut -d':' -f2)

# Extract host and port from ORIENTDB_URL
ORIENTDB_HOST=$(echo $ORIENTDB_URL | sed -E 's|^https?://([^:]+).*|\1|')
ORIENTDB_PORT=$(echo $ORIENTDB_URL | sed -E 's|^https?://[^:]+:([0-9]+).*|\1|')

echo "Checking Kafka at $KAFKA_HOST:$KAFKA_PORT"
echo "Checking OrientDB at $ORIENTDB_HOST:$ORIENTDB_PORT"
echo "Checking Redis at $REDIS_HOST:$REDIS_PORT"

# Function to check if a service is ready
wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service at $host:$port..."
    
    while ! nc -z $host $port >/dev/null 2>&1; do
        if [ $attempt -ge $max_attempts ]; then
            echo "Failed to connect to $service after $max_attempts attempts"
            exit 1
        fi
        
        echo "Attempt $attempt: $service is not available yet. Waiting 5 seconds..."
        sleep 5
        attempt=$((attempt+1))
    done
    
    echo "$service is available!"
}

# Wait for Kafka
wait_for_service $KAFKA_HOST $KAFKA_PORT "Kafka"

# Wait for OrientDB
wait_for_service $ORIENTDB_HOST $ORIENTDB_PORT "OrientDB"

# Wait for Redis
wait_for_service $REDIS_HOST $REDIS_PORT "Redis"

echo "All services are ready. Starting application..."