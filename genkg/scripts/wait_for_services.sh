#!/bin/bash
set -e

echo "Waiting for Kafka and OrientDB to be ready..."

# Check if environment variables are set
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "KAFKA_BOOTSTRAP_SERVERS is not set"
    exit 1
fi

if [ -z "$ORIENTDB_URL" ]; then
    echo "ORIENTDB_URL is not set"
    exit 1
fi

# Extract host and port from KAFKA_BOOTSTRAP_SERVERS
# Format expected: kafka1:19092,kafka2:19093,kafka3:19094
KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d',' -f1 | cut -d':' -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d',' -f1 | cut -d':' -f2)

# Extract host and port from ORIENTDB_URL
# Format expected: http://orientdb:2480
ORIENTDB_HOST=$(echo $ORIENTDB_URL | sed -E 's|^https?://([^:]+).*|\1|')
ORIENTDB_PORT=$(echo $ORIENTDB_URL | sed -E 's|^https?://[^:]+:([0-9]+).*|\1|')

echo "Checking Kafka at $KAFKA_HOST:$KAFKA_PORT"
echo "Checking OrientDB at $ORIENTDB_HOST:$ORIENTDB_PORT"

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

echo "All services are ready. Starting application..."