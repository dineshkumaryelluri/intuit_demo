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

  def run2(): Unit = {
    implicit val sparkSession = spark
    implicit val config = Config(
      processingMode = "batch",
      batchInputPath = "src/test/resources/data/input/sample_data2.json",
      tablePath = "src/test/resources/data/output/delta_table",
      writeMode = "merge",
      mergeKey = "id",
    )
    val processor = new BatchProcessor()
    processor.process()
            //val df = spark.read.format("json").load("src/test/resources/data/input/sample_data2.json")
            // var deltaTable = io.delta.tables.DeltaTable.forPath(spark, "src/test/resources/data/output/delta_table")
//          val mergeKey="id"
//            val out = deltaTable.as("target")
//              .merge(
//                  df.as("source"),
//                  s"target.$mergeKey = source.$mergeKey"
//              )
//              .whenMatched
//              .updateExpr(
//                  df.columns.filter(_ != mergeKey).map { col => (s"$col", s"coalesce(source.$col, target.$col)" )}.toMap
//              )
//              .whenNotMatched()
//              .insertAll()
//          out.execute()
    //      deltaTable = io.delta.tables.DeltaTable.forPath(spark, "src/test/resources/data/output/delta_table")
    //      deltaTable.toDF.show()
  }

  test("BatchProcessor should successfully process sample data") {
    try {
      // Initialize configuration
      implicit val config = Config(
        processingMode = "batch",
        batchInputPath = "src/test/resources/data/input/sample_data.json",
        tablePath = "src/test/resources/data/output/delta_table",
        writeMode = "merge",
        mergeKey = "id",
      )
      implicit val sparkSession = spark

      // Create and run batch processor
      val processor = new BatchProcessor()
      processor.process()
      val resultDF1 =io.delta.tables.DeltaTable.forPath(spark, "src/test/resources/data/output/delta_table").toDF
      resultDF1.show()
      run2()

      // Validate results
      val resultDF = io.delta.tables.DeltaTable.forPath(spark, "src/test/resources/data/output/delta_table").toDF

      // Basic validations
      assert(resultDF.count() == 2, "Expected 2 records in output")

      val namesDF = resultDF.select("name").collect()
      assert(namesDF.map(_.getString(0)).toSet == Set("John Doe", "Jane Smith"),
        "Expected names do not match")
      resultDF.show()
      logger.info("Batch processing test completed successfully")
    } catch {
      case e: Exception =>
        logger.error("Error during batch processing test", e)
        throw e
    }
  }
}

