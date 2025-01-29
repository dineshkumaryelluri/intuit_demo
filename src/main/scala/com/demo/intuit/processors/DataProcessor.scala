package com.demo.intuit.processors

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.demo.intuit.config.Config

trait DataProcessor {
    def process()(implicit spark: SparkSession, config: Config): Unit = {
        try {
            val df = readSource()
            val validatedDF = validateSchema(df)
            writeToTarget(validatedDF)
        } catch {
            case e: Exception =>
                println(s"Error processing data: ${e.getMessage}")
                throw e
        }
    }
    
    protected def readSource()(implicit spark: SparkSession, config: Config): DataFrame
    
    protected def validateSchema(df: DataFrame)(implicit spark: SparkSession, config: Config): DataFrame
    
    protected def writeToTarget(df: DataFrame)(implicit spark: SparkSession, config: Config): Unit
}
