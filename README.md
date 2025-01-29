# Data Processing Application

A robust Spark application that supports both batch and streaming data processing with Delta Lake integration and automatic schema evolution capabilities.

## Features

- Unified data processing for both batch and streaming workloads
- Automatic schema evolution with validation
- Delta Lake integration for ACID transactions
- Kafka streaming support
- JSON file batch processing
- Configurable merge/upsert operations

## Prerequisites

- Java 8 or higher
- Maven 3.6+
- Spark 3.x
- Kafka (for streaming mode)
- Schema Registry (optional, for schema evolution)

## Project Structure

```
src/
├── main/
│   ├── resources/
│   │   ├── application.conf
│   │   └── schema.json
│   └── scala/
│       └── com/
│           └── demo/
│               └── intuit/
│                   ├── config/
│                   │   └── Config.scala              # Configuration management
│                   ├── processors/
│                   │   ├── DataProcessor.scala       # Base processor trait
│                   │   ├── BatchProcessor.scala      # Batch processing logic
│                   │   └── StreamProcessor.scala     # Streaming processing logic
│                   ├── utils/
│                   │   ├── SchemaUtils.scala        # Schema evolution handling
│                   │   └── DeltaTableUtils.scala    # Delta Lake operations
│                   └── DataProcessingApp.scala       # Main application entry point
```

## Build Instructions

1. Clone the repository:
```bash
git clone <repository-url>
cd <project-directory>
```

2. Build with Maven:
```bash
mvn clean package
```
### Run test case 
```bash
  mvn clean compile test-compile scalatest:test 
```

The build will create an uber jar in the `target` directory.

## Running the Application

### Batch Mode

```bash
spark-submit \
--class com.demo.intuit.DataProcessingApp \
--master <spark-master-url> \
target/intuit-demo-1.0-SNAPSHOT.jar \
--processingMode batch \
--batchInputPath /path/to/input/json/files \
--batchMode incremental \
--targetTable my_table
```

### Streaming Mode

```bash
spark-submit \
--class com.demo.intuit.DataProcessingApp \
--master <spark-master-url> \
target/intuit-demo-1.0-SNAPSHOT.jar \
--processingMode streaming \
--kafkaBootstrapServers localhost:9092 \
--kafkaTopic input-topic \
--targetTable my_table
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| processingMode | Processing mode (batch/streaming) | batch |
| batchInputPath | Input path for JSON files in batch mode | |
| batchMode | Batch processing mode (incremental/full) | incremental |
| kafkaBootstrapServers | Kafka bootstrap servers | localhost:9092 |
| kafkaTopic | Kafka topic name | input-topic |
| targetTable | Target Delta table name | output_table |
| schemaRegistryUrl | Schema registry URL (optional) | |
| schemaEvolutionEnabled | Enable automatic schema evolution | true |

## Schema Evolution

The application supports automatic schema evolution with the following features:

- Automatic detection of new columns
- Schema validation against existing schema
- Schema versioning and compatibility checks
- Optional human approval workflow through schema registry
- Logging of all schema changes

To disable automatic schema evolution, set `--schemaEvolutionEnabled false`

## Delta Lake Features

The application leverages Delta Lake capabilities including:

- ACID transactions
- Upsert/merge operations
- Schema enforcement and evolution
- Time travel (historical versions)
- Optimization and compaction

## Example Commands

1. Run batch processing with schema evolution:
```bash
spark-submit \
--class com.demo.intuit.DataProcessingApp \
--master local[*] \
target/intuit-demo-1.0-SNAPSHOT.jar \
--processingMode batch \
--batchInputPath /data/input \
--schemaEvolutionEnabled true \
--targetTable customer_data
```

2. Run streaming processing with schema registry:
```bash
spark-submit \
--class com.demo.intuit.DataProcessingApp \
--master local[*] \
target/intuit-demo-1.0-SNAPSHOT.jar \
--processingMode streaming \
--kafkaBootstrapServers kafka:9092 \
--kafkaTopic customer-events \
--schemaRegistryUrl http://schema-registry:8081 \
--targetTable customer_data
```

## Notes

- Ensure sufficient memory is allocated to Spark driver and executors
- Monitor schema evolution logs for any validation failures
- Regular Delta table optimization is recommended for better performance
- Configure checkpoint location for streaming workloads
- Back up Delta table logs periodically

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

