# Glutton ETL Job - CSV to OrientDB Import

## Overview
Glutton is a Flink-based ETL job that reads CSV files and imports them into OrientDB tables. The job consumes messages from Kafka, extracts path information, and processes CSV files from the specified location.

## Features
- Consumes configuration from Kafka topics
- Dynamically processes CSV files from specified directories
- Automatic table creation based on CSV filename
- Schema inference from CSV headers
- Memory-optimized processing for large files
- Proper SQL escaping to prevent injection and syntax errors
- Configurable restart strategy

## Configuration
Configurable via environment variables:
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka connection (default: kafka:9092)
- `KAFKA_TOPIC_INGESTION` - Topic to consume (default: kg-ingestion)
- `KAFKA_GROUP_ID` - Consumer group (default: glutton-group)
- `KAFKA_STARTING_OFFSET` - Start position (default: earliest)
- `ORIENTDB_URL` - OrientDB URL (default: http://orientdb:2480)
- `ORIENTDB_DB` - Database name (default: genkg)
- `ORIENTDB_USER` - Username (default: root)
- `ORIENTDB_PASSWORD` - Password (default: root)

## Build Commands

### Format Code
```bash
mvn spotless:apply
```

### Build Package
```bash
mvn clean package
```

### Kafka Message Format
```json
{
  "kg_id": "18d838d8-4d86-4abf-b929-f4ca661bebde",
  "source": {
    "type": "local_files",
    "path": "/opt/flink/test_data"
  }
}
```