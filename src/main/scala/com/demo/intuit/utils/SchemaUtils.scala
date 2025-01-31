package com.demo.intuit.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import io.delta.tables._
import scala.util.{Try, Success, Failure}

object SchemaUtils {
private val logger = LoggerFactory.getLogger(this.getClass)

def inferSchema(df: DataFrame)(implicit spark: SparkSession): StructType = {
    logger.info("Inferring schema from input data")
    Try {
        df.schema
    } match {
        case Success(schema) =>
            logger.info(s"Successfully inferred schema: ${schema.treeString}")
            schema
        case Failure(e) =>
            logger.error(s"Failed to infer schema: ${e.getMessage}")
            throw new RuntimeException("Schema inference failed", e)
    }
}

def validateSchema(df: DataFrame, existingSchema: StructType)(implicit spark: SparkSession): DataFrame = {
    logger.info("Validating schema compatibility")
    Try {
        val currentSchema = df.schema
        if (currentSchema != existingSchema) {
            // Check if new schema is compatible with existing schema
            currentSchema.fields.foreach { field =>
                existingSchema.fields.find(_.name == field.name).foreach { existingField =>
                    if (field.dataType.catalogString != existingField.dataType.catalogString) {
                        throw new IllegalArgumentException(
                            s"Incompatible type change for field ${field.name}: ${existingField.dataType} -> ${field.dataType}"
                        )
                    }
                }
            }
        }
        df
    } match {
        case Success(validatedDf) =>
            logger.info("Schema validation successful")
            validatedDf
        case Failure(e) =>
            logger.error(s"Schema validation failed: ${e.getMessage}")
            throw e
    }
}

def evolveSchema(df: DataFrame, tablePath: String)(implicit spark: SparkSession): DataFrame = {
    Try {
        if (tableExists(tablePath)) {
            logger.info("Applying Delta Lake schema evolution")
            val deltaTable = DeltaTable.forPath(spark, tablePath)
            // Merge schema to enable evolution
            deltaTable.toDF.write.mode("overwrite")
                .option("mergeSchema", "true")
                .format("delta")
                .save(tablePath)
            df
        } else {
            logger.info("Creating new table with inferred schema")
            df.write.format("delta").save(tablePath)
            df
        }
    } match {
        case Success(evolvedDf) =>
            logger.info("Schema evolution completed successfully")
            evolvedDf
        case Failure(e) =>
            logger.error(s"Schema evolution failed: ${e.getMessage}")
            throw new RuntimeException("Schema evolution failed", e)
    }
}

def validateAndEvolveSchema(df: DataFrame, tablePath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Starting schema validation and evolution for table: $tablePath")
    Try {
        val currentSchema = getCurrentSchema(tablePath, df)
        val validatedDf = validateSchema(df, currentSchema)
        evolveSchema(validatedDf, tablePath)
    } match {
        case Success(result) =>
            logger.info("Schema validation and evolution completed successfully")
            result
        case Failure(e) =>
            logger.error(s"Schema validation and evolution failed: ${e.getMessage}")
            throw e
    }
}

def getCurrentSchema(tablePath: String, sourceDF: DataFrame)(implicit spark: SparkSession): StructType = {
    logger.info("Determining schema for table")
    Try {
        if (tableExists(tablePath)) {
            logger.info("Found existing Delta table, using its schema")
            DeltaTable.forPath(spark, tablePath).toDF.schema
        } else {
            logger.info("No existing Delta table found, inferring schema from source data")
            inferSchema(sourceDF)
        }
    } match {
        case Success(schema) =>
            logger.info(s"Schema determined successfully: ${schema.treeString}")
            schema
        case Failure(e) =>
            logger.error(s"Failed to determine schema: ${e.getMessage}")
            throw new RuntimeException("Schema determination failed", e)
    }
}

private def tableExists(tablePath: String)(implicit spark: SparkSession): Boolean = {
Try(DeltaTable.forPath(spark, tablePath)).isSuccess
}

}
