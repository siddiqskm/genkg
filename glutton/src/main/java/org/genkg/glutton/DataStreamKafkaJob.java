package org.genkg.glutton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class DataStreamKafkaJob {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamKafkaJob.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static JedisPool jedisPool;
  private static volatile boolean redisInitialized = false;
  private static final ThreadLocal<String> currentKgId = new ThreadLocal<>();

  // Config Classes
  public static class SourceConfig {
    public String type;
    public String path;
    public Object access_credentials;
  }

  public static class ColumnConfig {
    public String name;
    public String type;
    public boolean is_key;
  }

  public static class VertexConfig {
    public String name;
    public String file_pattern;
    public List<ColumnConfig> columns;
    public boolean distinct; // CHANGE #1: Added distinct field
  }

  public static class EdgeConfig {
    public String name;
    public String from_vertex;
    public String to_vertex;
    public String file_pattern;
    public List<ColumnConfig> properties;
    public MappingConfig mapping;
  }

  public static class MappingConfig {
    public String from_key;
    public String to_key;
  }

  public static class ETLConfig {
    public String kg_id;
    public SourceConfig source;
    public List<VertexConfig> vertices;
    public List<EdgeConfig> edges;
  }

  public static class GraphRecord {
    public enum RecordType {
      VERTEX,
      EDGE
    }

    public RecordType type;
    public String className;
    public Map<String, Object> properties;
    public String fromVertex;
    public String toVertex;
    public String fromKeyName; // Column name for from vertex
    public String toKeyName; // Column name for to vertex
    public Object fromKeyValue; // Actual value for from vertex
    public Object toKeyValue; // Actual value for to vertex

    // Constructor for Vertex
    public GraphRecord(String className, Map<String, Object> properties) {
      this.type = RecordType.VERTEX;
      this.className = className;
      this.properties = properties;
    }

    // Constructor for Edge
    public GraphRecord(
        String className,
        String fromVertex,
        String toVertex,
        String fromKeyName,
        String toKeyName,
        Map<String, Object> properties) {
      this.type = RecordType.EDGE;
      this.className = className;
      this.fromVertex = fromVertex;
      this.toVertex = toVertex;
      this.fromKeyName = fromKeyName;
      this.toKeyName = toKeyName;
      this.properties = properties;

      // Extract actual values from properties
      this.fromKeyValue = properties.get(fromKeyName);
      this.toKeyValue = properties.get(toKeyName);
    }
  }

  // Graph Data Processor
  public static class GraphDataProcessor extends ProcessFunction<ETLConfig, GraphRecord> {
    private transient MapState<String, Boolean> processedFiles;
    private transient Map<String, Set<String>> distinctVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      processedFiles =
          getRuntimeContext()
              .getMapState(new MapStateDescriptor<>("processedFiles", String.class, Boolean.class));
      distinctVertices = new HashMap<>();
    }

    @Override
    public void processElement(ETLConfig config, Context ctx, Collector<GraphRecord> out)
        throws Exception {
      if (config == null || config.source == null || config.source.path == null) {
        LOG.error("Invalid config or missing path");
        return;
      }

      currentKgId.set(config.kg_id);

      String basePath = config.source.path;
      LOG.info("Processing files from path: {}", basePath);

      File directory = new File(basePath);
      if (!directory.exists() || !directory.isDirectory()) {
        LOG.error("Directory does not exist: {}", basePath);
        try {
          updateJobStatus(config.kg_id, "FAILED", "Directory does not exist: " + basePath);
        } catch (Exception e) {
          LOG.error("Error updating Redis failure status: {}", e.getMessage());
        }
        return;
      }

      try {
        // Process each file according to the vertices and edges definitions
        processVertices(config, directory, out);
        processEdges(config, directory, out);

        // Update Redis status to SUCCESS
        updateJobStatus(config.kg_id, "SUCCESS", null);
      } catch (Exception e) {
        LOG.error("Error processing data: {}", e.getMessage());
        try {
          updateJobStatus(config.kg_id, "FAILED", e.getMessage());
        } catch (Exception ex) {
          LOG.error("Error updating Redis failure status: {}", ex.getMessage());
        }
        throw e;
      }
    }

    private void processVertices(ETLConfig config, File directory, Collector<GraphRecord> out)
        throws Exception {
      if (config.vertices == null || config.vertices.isEmpty()) {
        LOG.warn("No vertices defined in config");
        return;
      }

      for (VertexConfig vertexConfig : config.vertices) {
        String filePattern = vertexConfig.file_pattern;
        File[] matchingFiles = directory.listFiles((dir, name) -> name.equals(filePattern));

        if (matchingFiles == null || matchingFiles.length == 0) {
          LOG.warn(
              "No files matching pattern '{}' found for vertex '{}'",
              filePattern,
              vertexConfig.name);
          continue;
        }

        for (File file : matchingFiles) {
          String filePath = file.getAbsolutePath();
          String fileKey = vertexConfig.name + ":" + filePath;

          // Skip if already processed to prevent duplicate processing
          if (processedFiles.contains(fileKey)) {
            LOG.debug(
                "Skipping already processed file for vertex {}: {}", vertexConfig.name, filePath);
            continue;
          }

          LOG.info("Processing file for vertex {}: {}", vertexConfig.name, file.getName());

          try (java.io.BufferedReader reader =
              java.nio.file.Files.newBufferedReader(file.toPath())) {
            // Read header line
            String headerLine = reader.readLine();
            if (headerLine == null) {
              LOG.warn("Empty file: {}", file.getName());
              continue;
            }

            // Parse header
            String[] headers = headerLine.split(",");

            // Initialize distinct tracking for this vertex if needed
            if (vertexConfig.distinct && !distinctVertices.containsKey(vertexConfig.name)) {
              distinctVertices.put(vertexConfig.name, new HashSet<>());
            }

            // Process data rows
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
              lineNumber++;
              if (line.trim().isEmpty()) {
                continue;
              }

              String[] values = line.split(",");
              if (values.length != headers.length) {
                LOG.warn(
                    "Column count mismatch in file {} line {}: expected {} got {}",
                    file.getName(),
                    lineNumber,
                    headers.length,
                    values.length);
                continue;
              }

              // Create properties map for the vertex
              Map<String, Object> properties = new HashMap<>();
              String vertexKey = null;

              // Extract properties from columns defined in vertex config
              for (ColumnConfig column : vertexConfig.columns) {
                int columnIdx = getColumnIndex(headers, column.name);
                if (columnIdx >= 0 && columnIdx < values.length) {
                  Object value = convertValue(values[columnIdx].trim(), column.type);
                  properties.put(column.name, value);

                  // Track the key column for distinct checking
                  if (column.is_key) {
                    vertexKey = column.name + ":" + values[columnIdx].trim();
                  }
                }
              }

              // Skip if distinct is required and vertex already exists
              if (vertexConfig.distinct && vertexKey != null) {
                Set<String> keys = distinctVertices.get(vertexConfig.name);
                if (keys.contains(vertexKey)) {
                  continue;
                }
                keys.add(vertexKey);
              }

              // Emit vertex record
              out.collect(new GraphRecord(vertexConfig.name, properties));
            }
          } catch (Exception e) {
            LOG.error(
                "Error processing file {} for vertex {}: {}",
                file.getName(),
                vertexConfig.name,
                e.getMessage());
          }

          processedFiles.put(fileKey, true);
          LOG.info(
              "Successfully processed file for vertex {}: {}", vertexConfig.name, file.getName());
        }
      }
    }

    private void processEdges(ETLConfig config, File directory, Collector<GraphRecord> out)
        throws Exception {
      if (config.edges == null || config.edges.isEmpty()) {
        LOG.warn("No edges defined in config");
        return;
      }

      for (EdgeConfig edgeConfig : config.edges) {
        String filePattern = edgeConfig.file_pattern;
        File[] matchingFiles = directory.listFiles((dir, name) -> name.equals(filePattern));

        if (matchingFiles == null || matchingFiles.length == 0) {
          LOG.warn(
              "No files matching pattern '{}' found for edge '{}'", filePattern, edgeConfig.name);
          continue;
        }

        for (File file : matchingFiles) {
          String filePath = file.getAbsolutePath();
          String fileKey = edgeConfig.name + ":" + filePath;

          // Skip if already processed to prevent duplicate processing
          if (processedFiles.contains(fileKey)) {
            LOG.debug("Skipping already processed file for edge {}: {}", edgeConfig.name, filePath);
            continue;
          }

          LOG.info("Processing file for edge {}: {}", edgeConfig.name, file.getName());

          try (java.io.BufferedReader reader =
              java.nio.file.Files.newBufferedReader(file.toPath())) {
            // Read header line
            String headerLine = reader.readLine();
            if (headerLine == null) {
              LOG.warn("Empty file: {}", file.getName());
              continue;
            }

            // Parse header
            String[] headers = headerLine.split(",");

            // Process data rows
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
              lineNumber++;
              if (line.trim().isEmpty()) {
                continue;
              }

              String[] values = line.split(",");
              if (values.length != headers.length) {
                LOG.warn(
                    "Column count mismatch in file {} line {}: expected {} got {}",
                    file.getName(),
                    lineNumber,
                    headers.length,
                    values.length);
                continue;
              }

              // Get mapping values - here's the fix
              int fromKeyIdx = getColumnIndex(headers, edgeConfig.mapping.from_key);
              int toKeyIdx = getColumnIndex(headers, edgeConfig.mapping.to_key);

              if (fromKeyIdx < 0
                  || toKeyIdx < 0
                  || fromKeyIdx >= values.length
                  || toKeyIdx >= values.length) {
                LOG.warn("Mapping keys not found or out of bounds in line {}", lineNumber);
                continue;
              }

              // Store both the column names and the actual values
              String fromKeyName = edgeConfig.mapping.from_key;
              String toKeyName = edgeConfig.mapping.to_key;
              Object fromIdValue = values[fromKeyIdx].trim();
              Object toIdValue = values[toKeyIdx].trim();

              // Create properties map for the edge
              Map<String, Object> properties = new HashMap<>();

              // Add the key values to properties for later reference
              properties.put(fromKeyName, fromIdValue);
              properties.put(toKeyName, toIdValue);

              // Extract properties from edge properties configuration
              if (edgeConfig.properties != null) {
                for (ColumnConfig property : edgeConfig.properties) {
                  int propIdx = getColumnIndex(headers, property.name);
                  if (propIdx >= 0 && propIdx < values.length) {
                    Object value = convertValue(values[propIdx].trim(), property.type);
                    properties.put(property.name, value);
                  }
                }
              }

              // Emit edge record with both column names and values
              out.collect(
                  new GraphRecord(
                      edgeConfig.name,
                      edgeConfig.from_vertex,
                      edgeConfig.to_vertex,
                      fromKeyName,
                      toKeyName,
                      properties));
            }
          } catch (Exception e) {
            LOG.error(
                "Error processing file {} for edge {}: {}",
                file.getName(),
                edgeConfig.name,
                e.getMessage());
          }

          processedFiles.put(fileKey, true);
          LOG.info("Successfully processed file for edge {}: {}", edgeConfig.name, file.getName());
        }
      }
    }

    private int getColumnIndex(String[] headers, String columnName) {
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].trim().equals(columnName)) {
          return i;
        }
      }
      LOG.warn("Column '{}' not found in headers", columnName);
      return -1;
    }

    private Object convertValue(String value, String type) {
      if (value == null || value.isEmpty()) {
        return null;
      }

      try {
        switch (type.toLowerCase()) {
          case "integer":
          case "int":
            return Integer.parseInt(value);
          case "long":
            return Long.parseLong(value);
          case "float":
            return Float.parseFloat(value);
          case "double":
            return Double.parseDouble(value);
          case "boolean":
            return Boolean.parseBoolean(value);
          case "string":
          default:
            return value;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Failed to convert value '{}' to type {}: {}", value, type, e.getMessage());
        return value; // Return as string if conversion fails
      }
    }
  }

  // OrientDB Sink
  public static class OrientDBSink implements Sink<GraphRecord> {
    @Override
    public SinkWriter<GraphRecord> createWriter(Sink.InitContext context) throws IOException {
      return new OrientDBWriter();
    }

    private static class OrientDBWriter implements SinkWriter<GraphRecord> {
      private transient OrientDB client;
      private transient ODatabaseSession session;
      private final Map<String, Map<Object, String>> vertexRidCache = new HashMap<>();

      private void initializeIfNeeded() {
        if (client == null) {
          String orientdbUrl = System.getenv("ORIENTDB_URL");
          String orientdbDb = System.getenv("ORIENTDB_DB");
          String orientdbUser = System.getenv("ORIENTDB_USER");
          String orientdbPassword = System.getenv("ORIENTDB_PASSWORD");

          // Parse URL to get host and port for OrientDB connection
          String orientdbHost = "orientdb";
          int orientdbPort = 2424;

          try {
            if (orientdbUrl != null && !orientdbUrl.isEmpty()) {
              java.net.URL url = new java.net.URL(orientdbUrl);
              orientdbHost = url.getHost();
              // Use binary protocol port (2424) instead of HTTP port (2480)
              orientdbPort = 2424;
            }
          } catch (Exception e) {
            LOG.warn("Failed to parse ORIENTDB_URL, using default: {}", e.getMessage());
          }

          // Use defaults if environment variables are not set
          if (orientdbDb == null || orientdbDb.isEmpty()) {
            orientdbDb = "genkg";
            LOG.warn("ORIENTDB_DB not set, using default: {}", orientdbDb);
          }

          if (orientdbUser == null || orientdbUser.isEmpty()) {
            orientdbUser = "root";
            LOG.warn("ORIENTDB_USER not set, using default: {}", orientdbUser);
          }

          if (orientdbPassword == null || orientdbPassword.isEmpty()) {
            orientdbPassword = "root";
            LOG.warn("ORIENTDB_PASSWORD not set, using default: {}", orientdbPassword);
          }

          String orientdbConnectionString =
              String.format("remote:%s:%d", orientdbHost, orientdbPort);
          LOG.info(
              "Connecting to OrientDB at: {}, database: {}", orientdbConnectionString, orientdbDb);

          // Implement connection retry logic
          int maxRetries = 5;
          int retryCount = 0;
          int retryDelayMs = 5000; // 5 seconds initial delay
          boolean connected = false;

          while (!connected && retryCount < maxRetries) {
            try {
              if (retryCount > 0) {
                LOG.info(
                    "Retry attempt {} connecting to OrientDB at {}",
                    retryCount,
                    orientdbConnectionString);
              }

              client = new OrientDB(orientdbConnectionString, OrientDBConfig.defaultConfig());
              session = client.open(orientdbDb, orientdbUser, orientdbPassword);
              connected = true;
              LOG.info("Successfully connected to OrientDB");
            } catch (Exception e) {
              retryCount++;
              if (retryCount >= maxRetries) {
                LOG.error(
                    "Failed to connect to OrientDB after {} attempts: {}",
                    maxRetries,
                    e.getMessage());
                throw e;
              } else {
                LOG.warn(
                    "Connection attempt {} failed: {}. Retrying in {} ms...",
                    retryCount,
                    e.getMessage(),
                    retryDelayMs);
                try {
                  Thread.sleep(retryDelayMs);
                  // Exponential backoff for next retry
                  retryDelayMs *= 2;
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException("Interrupted during connection retry", ie);
                }
              }
            }
          }
        }
      }

      @Override
      public void write(GraphRecord record, Context context)
          throws IOException, InterruptedException {
        try {
          initializeIfNeeded();

          if (record == null) {
            LOG.warn("Null record received");
            return;
          }

          if (record.type == GraphRecord.RecordType.VERTEX) {
            writeVertex(record);
          } else if (record.type == GraphRecord.RecordType.EDGE) {
            writeEdge(record);
          }
        } catch (Exception e) {
          LOG.error("Error writing to OrientDB: {}", e.getMessage(), e);
          try {
            updateJobStatus(
                currentKgId.get(), "FAILED", "Error writing to OrientDB: " + e.getMessage());
          } catch (Exception ex) {
            LOG.error("Error updating Redis failure status: {}", ex.getMessage());
          }
          throw new IOException("Failed to write to OrientDB", e);
        }
      }

      private void writeVertex(GraphRecord record) {
        try {
          // Check if vertex class exists, create if not
          if (!session.getMetadata().getSchema().existsClass(record.className)) {
            String createClassSQL = "CREATE CLASS " + record.className + " EXTENDS V";
            LOG.info("Executing SQL: {}", createClassSQL);
            session.command(createClassSQL).close();
            LOG.info("Created vertex class: {}", record.className);
          }

          // Build vertex insert SQL
          StringBuilder sql = new StringBuilder();
          sql.append("CREATE VERTEX ").append(record.className).append(" SET ");

          List<String> assignments = new ArrayList<>();
          Object keyValue = null;
          String keyProperty = null;

          for (Map.Entry<String, Object> entry : record.properties.entrySet()) {
            String property = entry.getKey();
            Object value = entry.getValue();

            // Find the key property if any
            for (Map.Entry<String, Object> propEntry : record.properties.entrySet()) {
              if (propEntry.getKey().toLowerCase().contains("id")) {
                keyProperty = propEntry.getKey();
                keyValue = propEntry.getValue();
                break;
              }
            }

            if (value != null) {
              assignments.add(String.format("%s = %s", property, formatSqlValue(value)));
            }
          }

          sql.append(String.join(", ", assignments));

          // Execute the query and get the RID
          LOG.info("Executing SQL: {}", sql.toString());
          try {
            // Execute the query and get the RID
            var result = session.command(sql.toString()).next();
            Object ridObj = result.getProperty("@rid");
            String vertexRid = ridObj.toString();
            LOG.debug("Inserted vertex {} with RID {}", record.className, vertexRid);

            // Cache the RID if we have a key value
            if (keyValue != null && keyProperty != null) {
              if (!vertexRidCache.containsKey(record.className)) {
                vertexRidCache.put(record.className, new HashMap<>());
              }
              vertexRidCache.get(record.className).put(keyValue, vertexRid);
              LOG.debug(
                  "Cached RID for {} with key {}:{}", record.className, keyProperty, keyValue);
            }
          } catch (Exception e) {
            LOG.error("Error getting RID after vertex creation: {}", e.getMessage(), e);
          }
        } catch (Exception e) {
          LOG.error("Error creating vertex {}: {}", record.className, e.getMessage(), e);
        }
      }

      private void writeEdge(GraphRecord record) {
        try {
          // Check if edge class exists, create if not
          if (!session.getMetadata().getSchema().existsClass(record.className)) {
            String createClassSQL = "CREATE CLASS " + record.className + " EXTENDS E";
            LOG.info("Executing SQL: {}", createClassSQL);
            session.command(createClassSQL).close();
            LOG.info("Created edge class: {}", record.className);
          }

          // Get the actual column names and values
          String fromKeyColumn = record.fromKeyName;
          String toKeyColumn = record.toKeyName;
          Object fromIdValue = record.fromKeyValue;
          Object toIdValue = record.toKeyValue;

          if (fromIdValue == null || toIdValue == null) {
            LOG.warn(
                "Missing key values for edge {}: fromKey={}, toKey={}",
                record.className,
                fromKeyColumn,
                toKeyColumn);
            return;
          }

          // Direct queries using the exact keys from the mapping configuration
          String findFromSQL =
              String.format(
                  "SELECT @rid FROM %s WHERE %s = %s",
                  record.fromVertex, fromKeyColumn, formatSqlValue(fromIdValue));

          String findToSQL =
              String.format(
                  "SELECT @rid FROM %s WHERE %s = %s",
                  record.toVertex, toKeyColumn, formatSqlValue(toIdValue));

          LOG.info("Finding from vertex: {}", findFromSQL);
          var fromResult = session.command(findFromSQL);

          LOG.info("Finding to vertex: {}", findToSQL);
          var toResult = session.command(findToSQL);

          String fromRid = null;
          String toRid = null;

          if (fromResult.hasNext()) {
            fromRid = fromResult.next().getProperty("@rid").toString();
            fromResult.close();
          } else {
            fromResult.close();
            LOG.warn(
                "From vertex not found: {}.{} = {}", record.fromVertex, fromKeyColumn, fromIdValue);
            return;
          }

          if (toResult.hasNext()) {
            toRid = toResult.next().getProperty("@rid").toString();
            toResult.close();
          } else {
            toResult.close();
            LOG.warn("To vertex not found: {}.{} = {}", record.toVertex, toKeyColumn, toIdValue);
            return;
          }

          // Build edge creation SQL
          StringBuilder sql = new StringBuilder();
          sql.append("CREATE EDGE ")
              .append(record.className)
              .append(" FROM ")
              .append(fromRid)
              .append(" TO ")
              .append(toRid);

          if (record.properties != null && !record.properties.isEmpty()) {
            sql.append(" SET ");
            List<String> assignments = new ArrayList<>();

            for (Map.Entry<String, Object> entry : record.properties.entrySet()) {
              String property = entry.getKey();
              Object value = entry.getValue();

              // Skip the key fields since they're used for edge relationships
              if (!property.equals(fromKeyColumn)
                  && !property.equals(toKeyColumn)
                  && value != null) {
                assignments.add(String.format("%s = %s", property, formatSqlValue(value)));
              }
            }

            if (!assignments.isEmpty()) {
              sql.append(String.join(", ", assignments));
            } else {
              // Remove the SET clause if no properties
              sql.delete(sql.length() - 4, sql.length());
            }
          }

          // Execute the query
          LOG.info("Executing SQL: {}", sql.toString());
          session.command(sql.toString()).close();
          LOG.debug("Created edge {} from {} to {}", record.className, fromRid, toRid);
        } catch (Exception e) {
          LOG.error("Error creating edge {}: {}", record.className, e.getMessage(), e);
        }
      }

      private String formatSqlValue(Object value) {
        if (value == null) {
          return "null";
        }

        if (value instanceof String) {
          return "'" + escapeSqlValue((String) value) + "'";
        } else if (value instanceof Number || value instanceof Boolean) {
          return value.toString();
        } else {
          return "'" + escapeSqlValue(value.toString()) + "'";
        }
      }

      private String escapeSqlValue(String value) {
        if (value == null) return "";

        // First escape backslashes
        value = value.replace("\\", "\\\\");

        // Then escape single quotes
        value = value.replace("'", "\\'");

        // Remove control characters
        value = value.replaceAll("[\\x00-\\x1F\\x7F]", "");

        return value;
      }

      @Override
      public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // OrientDB transactions are auto-committed
      }

      @Override
      public void close() throws Exception {
        if (session != null) {
          session.close();
          session = null;
        }
        if (client != null) {
          client.close();
          client = null;
        }
      }
    }
  }

  // Connection provider that ensures Redis is available in each task
  private static synchronized JedisPool getRedisConnection() {
    if (!redisInitialized) {
      try {
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        int redisDb = Integer.parseInt(System.getenv().getOrDefault("REDIS_DB", "0"));

        LOG.info("TASK-LEVEL REDIS INIT: {}:{} DB {}", redisHost, redisPort, redisDb);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);

        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);

        // Test connection
        try (Jedis jedis = jedisPool.getResource()) {
          String pingResponse = jedis.ping();
          LOG.info("TASK-LEVEL REDIS TEST: {}", pingResponse);
        }

        redisInitialized = true;
      } catch (Exception e) {
        LOG.error("TASK-LEVEL REDIS INIT FAILED: {}", e.getMessage());
        jedisPool = null;
      }
    }
    return jedisPool;
  }

  private static void updateJobStatus(String kgId, String status, String errorMessage) {
    if (kgId == null) return;

    JedisPool pool = getRedisConnection();
    if (pool == null) {
      LOG.error("Cannot update job status: Redis connection failed");
      return;
    }

    try (Jedis jedis = pool.getResource()) {
      String jobKey = "job:" + kgId;
      jedis.hset(jobKey, "status", status);
      jedis.hset(jobKey, "updated_at", Instant.now().toString());

      if (errorMessage != null && !errorMessage.isEmpty()) {
        jedis.hset(jobKey, "error", errorMessage);
      }

      LOG.info("Set job status to {} for kg_id: {}", status, kgId);
    } catch (Exception e) {
      LOG.error("Error updating Redis status: {}", e.getMessage(), e);
    }
  }

  public static void main(String[] args) throws Exception {
    System.err.println("STARTING APPLICATION");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Do not restart the job upon failures
    Configuration conf = new Configuration();
    conf.setString("restart-strategy", "none");
    env.configure(conf);

    // Enable checkpointing for consistent offset commits
    env.enableCheckpointing(5000); // Checkpoint every 5 seconds

    // Read Kafka configuration from environment variables
    String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    String topic = System.getenv("KAFKA_TOPIC_INGESTION");
    String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "glutton-group");
    String startingOffset = System.getenv().getOrDefault("KAFKA_STARTING_OFFSET", "latest");

    // Use defaults if environment variables are not set
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      bootstrapServers = "kafka:9092";
      LOG.warn("KAFKA_BOOTSTRAP_SERVERS not set, using default: {}", bootstrapServers);
    }

    if (topic == null || topic.isEmpty()) {
      topic = "kg-ingestion";
      LOG.warn("KAFKA_TOPIC_INGESTION not set, using default: {}", topic);
    }

    LOG.info("Using Kafka bootstrap servers: {}", bootstrapServers);
    LOG.info("Consuming from topic: {}", topic);
    LOG.info("Using consumer group ID: {}", groupId);
    LOG.info("Starting offset: {}", startingOffset);

    // Determine offset initializer based on configuration
    OffsetsInitializer offsetsInitializer;
    if (startingOffset.equalsIgnoreCase("latest")) {
      offsetsInitializer = OffsetsInitializer.latest();
    } else {
      offsetsInitializer = OffsetsInitializer.earliest();
    }
    LOG.info("Using offset initializer: {}", startingOffset);

    // Create Kafka Source with proper offset management
    KafkaSource<String> kafkaSource =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(offsetsInitializer)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    // Process incoming messages
    DataStream<String> kafkaStream =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Source");

    // Parse JSON payload
    DataStream<ETLConfig> configStream =
        kafkaStream
            .map(
                new MapFunction<String, ETLConfig>() {
                  @Override
                  public ETLConfig map(String value) throws Exception {
                    try {
                      LOG.info("INCOMING PAYLOAD: {}", value);
                      ETLConfig config = mapper.readValue(value, ETLConfig.class);
                      try {
                        String configJson = mapper.writeValueAsString(config);
                        LOG.info("PARSED CONFIG: {}", configJson);
                      } catch (Exception e) {
                        LOG.error("Error serializing config for logging: {}", e.getMessage());
                      }
                      return config;
                    } catch (Exception e) {
                      LOG.error("Error parsing JSON payload: {}", e.getMessage(), e);
                      return null;
                    }
                  }
                })
            .filter(Objects::nonNull);

    // Update status to PROCESSING when message is received
    configStream =
        configStream.map(
            new MapFunction<ETLConfig, ETLConfig>() {
              @Override
              public ETLConfig map(ETLConfig config) throws Exception {
                if (config != null && config.kg_id != null) {
                  // Use the updateJobStatus method
                  updateJobStatus(config.kg_id, "PROCESSING", null);

                  // Add extra timestamp information
                  JedisPool pool = getRedisConnection();
                  if (pool != null) {
                    try (Jedis jedis = pool.getResource()) {
                      String jobKey = "job:" + config.kg_id;
                      jedis.hset(jobKey, "processing_started", Instant.now().toString());
                    }
                  }
                }
                return config;
              }
            });

    // Process data and load to OrientDB
    configStream
        .keyBy(
            config -> config.kg_id != null ? config.kg_id : "default") // Key by knowledge graph ID
        .process(new GraphDataProcessor())
        .sinkTo(new OrientDBSink())
        .name("OrientDB-Sink");

    env.execute("Graph Data Import Job");
  }
}
