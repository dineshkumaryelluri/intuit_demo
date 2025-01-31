package com.demo.intuit.processors

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.demo.intuit.config.Config
import com.demo.intuit.utils.{SchemaUtils, DeltaTableUtils}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

class StreamProcessor extends DataProcessor {
    private val log = LoggerFactory.getLogger(getClass)
    
    override protected def readSource()(implicit spark: SparkSession, config: Config): DataFrame = {
        log.info(s"Reading streaming data from Kafka topic ${config.kafkaTopic}")
        
        // First read raw data to infer schema
        val rawDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
            .option("subscribe", config.kafkaTopic)
            .load()
            .selectExpr("CAST(value AS STRING) as json")

        // Get schema based on raw data
        val schema = SchemaUtils.getCurrentSchema(config.tablePath, rawDF)
        
        // Apply schema to raw data
        rawDF.select(from_json(col("json"), schema).as("data"))
            .select("data.*")
    }   
    
    override def process()(implicit spark: SparkSession, config: Config): Unit = {
        val streamDF = readSource
        
        val validatedDF = validateSchema(streamDF)
        writeToTarget(validatedDF)
    }
    
    override protected def validateSchema(df: DataFrame)(implicit spark: SparkSession, config: Config): DataFrame = {
        SchemaUtils.validateAndEvolveSchema(df, config.tablePath)
    }

    override protected def writeToTarget(df: DataFrame)(implicit spark: SparkSession, config: Config): Unit = {
        try {
            df.writeStream.foreachBatch((batchDF: DataFrame, batchId: Long) => {
                DeltaTableUtils.writeToTable(
                    batchDF,
                    config.tablePath,
                    "merge",
                    config.mergeKey,
                    None
                )
            }).start()
        } catch {
            case e: Exception =>
                log.error(s"Failed to write streaming data to target table: ${e.getMessage}")
                throw e
        }
    }
}
