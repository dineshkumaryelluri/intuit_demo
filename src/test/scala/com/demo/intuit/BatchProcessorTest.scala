package com.demo.intuit

import org.apache.spark.sql.SparkSession
import com.demo.intuit.config.Config
import com.demo.intuit.processors.BatchProcessor
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

class BatchProcessorTest extends AnyFunSuite with BeforeAndAfterAll {
private val logger = LoggerFactory.getLogger(getClass)
private var spark: SparkSession = _

override def beforeAll(): Unit = {
    spark = SparkSession.builder()
    .master("local[*]")
    .appName("BatchProcessorTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}

override def afterAll(): Unit = {
    if (spark != null) {
    spark.stop()
    }
}

test("BatchProcessor should successfully process sample data") {
    try {
    // Initialize configuration
    implicit val config = Config(
        processingMode = "batch",
        batchInputPath = "src/test/resources/data/input/sample_data.json",
        tablePath = "src/test/resources/data/output/delta_table",
        writeMode = "overwrite",
        mergeKey = "id",
    )
    implicit val sparkSession = spark

    // Create and run batch processor
    val processor = new BatchProcessor()
    processor.process()

    // Validate results
    val resultDF = spark.read.format("delta")
        .load(config.tablePath)

    // Basic validations
    assert(resultDF.count() == 2, "Expected 2 records in output")
    
    val namesDF = resultDF.select("name").collect()
    assert(namesDF.map(_.getString(0)).toSet == Set("John Doe", "Jane Smith"),
        "Expected names do not match")

    logger.info("Batch processing test completed successfully")
    } catch {
    case e: Exception =>
        logger.error("Error during batch processing test", e)
        throw e
    }
}
}

