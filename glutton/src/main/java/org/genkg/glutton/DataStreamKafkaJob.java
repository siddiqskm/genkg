package org.genkg.glutton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.io.File;
import java.io.IOException;
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

public class DataStreamKafkaJob {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamKafkaJob.class);
  private static final ObjectMapper mapper = new ObjectMapper();

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
  }

  public static class EdgeConfig {
    public String name;
    public String from_vertex;
    public String to_vertex;
    public String file_pattern;
    public List<ColumnConfig> properties;
    public Object mapping;
  }

  public static class ETLConfig {
    public String kg_id;
    public SourceConfig source;
    public List<VertexConfig> vertices;
    public List<EdgeConfig> edges;
  }

  public static class CsvRecord {
    public String tableName;
    public Map<String, String> data;

    public CsvRecord(String tableName, Map<String, String> data) {
      this.tableName = tableName;
      this.data = data;
    }
  }

  // CSV File Processor
  public static class CsvFileProcessor extends ProcessFunction<ETLConfig, CsvRecord> {
    private transient MapState<String, Boolean> processedFiles;

    @Override
    public void open(Configuration parameters) throws Exception {
      processedFiles =
          getRuntimeContext()
              .getMapState(new MapStateDescriptor<>("processedFiles", String.class, Boolean.class));
    }

    @Override
    public void processElement(ETLConfig config, Context ctx, Collector<CsvRecord> out)
        throws Exception {
      if (config == null || config.source == null || config.source.path == null) {
        LOG.error("Invalid config or missing path");
        return;
      }

      String basePath = config.source.path;
      LOG.info("Processing files from path: {}", basePath);

      File directory = new File(basePath);
      if (!directory.exists() || !directory.isDirectory()) {
        LOG.error("Directory does not exist: {}", basePath);
        return;
      }

      // Get all CSV files in the directory
      File[] csvFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
      if (csvFiles == null || csvFiles.length == 0) {
        LOG.warn("No CSV files found in directory: {}", basePath);
        return;
      }

      // Process each CSV file
      for (File csvFile : csvFiles) {
        String filePath = csvFile.getAbsolutePath();

        // Skip if already processed to prevent duplicate processing
        if (processedFiles.contains(filePath)) {
          LOG.debug("Skipping already processed file: {}", filePath);
          continue;
        }

        LOG.info("Processing CSV file: {}", csvFile.getName());

        // Table name is the file name without .csv extension
        String tableName = csvFile.getName().substring(0, csvFile.getName().lastIndexOf('.'));

        try (java.io.BufferedReader reader =
            java.nio.file.Files.newBufferedReader(csvFile.toPath())) {
          // Read header line
          String headerLine = reader.readLine();
          if (headerLine == null) {
            LOG.warn("Empty CSV file: {}", csvFile.getName());
            continue;
          }

          // Parse header
          String[] headers = headerLine.split(",");

          // Process data rows line by line instead of storing all lines in memory
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
                  csvFile.getName(),
                  lineNumber,
                  headers.length,
                  values.length);
              continue;
            }

            // Create data map
            Map<String, String> data = new HashMap<>();
            for (int j = 0; j < headers.length; j++) {
              // Extra check to prevent array index issues
              data.put(headers[j].trim(), j < values.length ? values[j].trim() : "");
            }

            // Emit record
            out.collect(new CsvRecord(tableName, data));
          }
          // Proper exception handling for file processing errors
        } catch (Exception e) {
          LOG.error("Error processing CSV file {}: {}", csvFile.getName(), e.getMessage());
        }

        processedFiles.put(filePath, true);
        LOG.info("Successfully processed and marked file: {}", filePath);
      }
    }
  }

  // OrientDB Sink
  public static class OrientDBSink implements Sink<CsvRecord> {
    @Override
    public SinkWriter<CsvRecord> createWriter(Sink.InitContext context) throws IOException {
      return new OrientDBWriter();
    }

    private static class OrientDBWriter implements SinkWriter<CsvRecord> {
      private transient OrientDB client;
      private transient ODatabaseSession session;

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

          client = new OrientDB(orientdbConnectionString, OrientDBConfig.defaultConfig());
          session = client.open(orientdbDb, orientdbUser, orientdbPassword);
        }
      }

      @Override
      public void write(CsvRecord record, Context context)
          throws IOException, InterruptedException {
        try {
          initializeIfNeeded();

          if (record == null || record.tableName == null || record.data == null) {
            LOG.warn("Invalid record received");
            return;
          }

          // Create SQL INSERT statement
          StringBuilder sql = new StringBuilder();
          sql.append("INSERT INTO ").append(record.tableName).append(" SET ");

          List<String> assignments = new ArrayList<>();
          for (Map.Entry<String, String> entry : record.data.entrySet()) {
            String value = escapeSqlValue(entry.getValue());
            assignments.add(String.format("%s = '%s'", entry.getKey(), value));
          }

          sql.append(String.join(", ", assignments));

          // Execute the query
          session.command(sql.toString()).stream().close();
          LOG.info("Inserted record into table {}", record.tableName);

        } catch (Exception e) {
          LOG.error("Error writing to OrientDB: {}", e.getMessage(), e);
          throw new IOException("Failed to write to OrientDB", e);
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

  public static void main(String[] args) throws Exception {
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
                      return mapper.readValue(value, ETLConfig.class);
                    } catch (Exception e) {
                      LOG.error("Error parsing JSON payload: {}", e.getMessage(), e);
                      return null;
                    }
                  }
                })
            .filter(Objects::nonNull);

    // Process CSV files and load to OrientDB
    // configStream.process(new CsvFileProcessor()).sinkTo(new
    // OrientDBSink()).name("OrientDB-Sink");
    configStream
        .keyBy(
            config -> config.kg_id != null ? config.kg_id : "default") // Key by knowledge graph ID
        .process(new CsvFileProcessor())
        .sinkTo(new OrientDBSink())
        .name("OrientDB-Sink");

    env.execute("CSV to OrientDB Import Job");
  }
}
