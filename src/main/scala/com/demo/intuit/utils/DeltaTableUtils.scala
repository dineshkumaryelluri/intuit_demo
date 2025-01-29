package com.demo.intuit.utils

import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object DeltaTableUtils {
private val logger = LoggerFactory.getLogger(this.getClass)

def initializeTable(
    df: DataFrame,
    tablePath: String,
    partitionColumns: Seq[String] = Seq.empty
)(implicit spark: SparkSession): Unit = {
    try {
    logger.info(s"Initializing Delta table at $tablePath")
    val writer = df.write
        .format("delta")
        .mode(SaveMode.ErrorIfExists)

    if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns: _*)
    }

    writer.save(tablePath)
    logger.info(s"Successfully initialized Delta table at $tablePath")
    } catch {
    case e: Exception =>
        logger.error(s"Failed to initialize Delta table at $tablePath", e)
        throw e
    }
}

def writeToTable(
    df: DataFrame,
    tablePath: String,
    writeMode: String = "merge",
    mergeKey: String,
    updateCondition: Option[String] = None
): Unit = {
    try {
        implicit val spark = df.sparkSession
    writeMode.toLowerCase match {
        case "merge" => mergeData(df, tablePath, mergeKey, updateCondition)
        case "append" => appendData(df, tablePath)
        case "overwrite" => overwriteData(df, tablePath)
        case _ => throw new IllegalArgumentException(s"Unsupported write mode: $writeMode")
    }
    } catch {
    case e: Exception =>
        logger.error(s"Failed to write data to Delta table at $tablePath", e)
        throw e
    }
}

private def mergeData(
    df: DataFrame,
    tablePath: String,
    mergeKey: String,
    updateCondition: Option[String]
)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting merge operation on table: $tablePath")
    logger.info(s"Merging data using the following columns:${df.columns.filter(_ != mergeKey).map { col => (s"$col", s"coalesce(target.$col, source.$col)" )}.toMap}")
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    val mergeBuilder = deltaTable.as("target")
      .merge(
          df.as("source"),
          s"target.$mergeKey = source.$mergeKey"
      )
      .whenMatched
      .updateExpr(
          df.columns.filter(_ != mergeKey).map { col => (s"$col", s"coalesce(source.$col, target.$col)" )}.toMap
      )
      .whenNotMatched()
      .insertAll()
    mergeBuilder.execute()
      //.whenNotMatchedBySource().delete()

    logger.info(s"Successfully completed merge operation on table: $tablePath")
}

private def appendData(df: DataFrame, tablePath: String): Unit = {
    logger.info(s"Appending data to table: $tablePath")
    df.write
    .format("delta")
    .mode(SaveMode.Append)
    .save(tablePath)
}

private def overwriteData(df: DataFrame, tablePath: String): Unit = {
    logger.info(s"Overwriting data in table: $tablePath")
    df.write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(tablePath)
}

def optimizeTable(
    tablePath: String,
    zOrderByColumns: Seq[String] = Seq.empty
)(implicit spark: SparkSession): Unit = {
    try {
    logger.info(s"Starting optimization for table: $tablePath")
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    
    if (zOrderByColumns.nonEmpty) {
        deltaTable.optimize()
        .executeZOrderBy(zOrderByColumns: _*)
    } else {
        deltaTable.optimize()
        .executeCompaction()
    }
    logger.info(s"Successfully optimized table: $tablePath")
    } catch {
    case e: Exception =>
        logger.error(s"Failed to optimize table: $tablePath", e)
        throw e
    }
}

def vacuumTable(
    tablePath: String,
    retentionHours: Option[Double] = None
)(implicit spark: SparkSession): Unit = {
    try {
    logger.info(s"Starting vacuum operation for table: $tablePath")
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    
    retentionHours match {
        case Some(hours) => deltaTable.vacuum(hours)
        case None => deltaTable.vacuum()
    }
    logger.info(s"Successfully vacuumed table: $tablePath")
    } catch {
    case e: Exception =>
        logger.error(s"Failed to vacuum table: $tablePath", e)
        throw e
    }
}

def tableExists(tablePath: String)(implicit spark: SparkSession): Boolean = {
    try {
    DeltaTable.isDeltaTable(spark, tablePath)
    } catch {
    case _: Exception => false
    }
}
}
