package com.demo.intuit.config

case class Config(
    // Processing configuration
    processingMode: String = "batch",     // Valid values: "batch" or "streaming"
    
    // Batch processing configuration
    batchInputPath: String = "",          // Input path for batch JSON files
    batchMode: String = "incremental",    // Valid values: "incremental" or "full"
    
    // Kafka configuration for streaming
    kafkaBootstrapServers: String = "localhost:9092",
    kafkaTopic: String = "input-topic",
    
    // Delta Lake configuration
    tablePath: String = "",               // Path to Delta table
    writeMode: String = "merge",          // Valid values: "merge", "append", "overwrite"
    mergeKey: String = "id",             // Column used for merging records
    
    // Schema configuration
    schemaRegistryUrl: String = "",
    schemaEvolutionEnabled: Boolean = true
)

object Config {
    def parseArgs(args: Array[String]): Config = {
        // Implementation of argument parsing
        Config()
    }
}
