package com.demo.intuit.processors

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.demo.intuit.config.Config
import com.demo.intuit.utils.{SchemaUtils, DeltaTableUtils}
import org.slf4j.LoggerFactory

class BatchProcessor extends DataProcessor {
    private val log = LoggerFactory.getLogger(getClass)
    
    override protected def readSource()(implicit spark: SparkSession, config: Config): DataFrame = {
        log.info(s"Reading batch data from ${config.batchInputPath}")
        spark.read
            .format("json")
            .load(config.batchInputPath)
    }
    
    override def process()(implicit spark: SparkSession, config: Config): Unit = {
        val inputDF = readSource
        
        val validatedDF = validateSchema(inputDF)
        writeToTarget(validatedDF)
    }
    
    override protected def validateSchema(df: DataFrame)(implicit spark: SparkSession, config: Config): DataFrame = {
        SchemaUtils.validateAndEvolveSchema(df, config.tablePath)
    }

    override protected def writeToTarget(df: DataFrame)(implicit spark: SparkSession, config: Config): Unit = {
        try {
            log.info(s"Writing batch data to target table ${config.tablePath}")
            DeltaTableUtils.writeToTable(
                df,
                config.tablePath,
                config.writeMode,
                config.mergeKey
            )
            log.info("Successfully wrote batch data to target table")
        } catch {
            case e: Exception => 
                log.error(s"Failed to write batch data to target table: ${e.getMessage}")
                throw e
        }
    }
}
