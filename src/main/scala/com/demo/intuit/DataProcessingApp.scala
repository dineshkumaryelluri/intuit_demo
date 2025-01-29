package com.demo.intuit

import org.apache.spark.sql.SparkSession
import com.demo.intuit.config.Config
import com.demo.intuit.processors.{BatchProcessor, StreamProcessor}

object DataProcessingApp {
    def main(args: Array[String]): Unit = {
        implicit val config: Config = Config.parseArgs(args)
        
        implicit val spark: SparkSession = SparkSession.builder()
            .appName("Data Processing Application")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .getOrCreate()
        
        val processor = config.processingMode match {
            case "batch" => new BatchProcessor
            case "streaming" => new StreamProcessor
            case _ => throw new IllegalArgumentException("Invalid processing mode")
        }
        
        processor.process()
    }
}
