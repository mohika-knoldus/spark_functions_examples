package org.knoldus.assignmentQues

import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.types._

object explodeFunc extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("explode array into columns")
    .getOrCreate()

  val schema = StructType(Seq(
    StructField("business_id", StringType),
    StructField("full_address", StringType),
    StructField("hours", StructType(Seq(
      StructField("Monday", StructType(Seq(
        StructField("close_time", StringType),
        StructField("open_time", StringType)
      )))))),
    StructField("Tuesday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    ))),
    StructField("Friday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    ))),
    StructField("Wednesday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    ))),
    StructField("Thursday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    ))),
    StructField("Sunday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    ))),
    StructField("Saturday", StructType(Seq(
      StructField("close_time", StringType),
      StructField("open_time", StringType)
    )))
  ))

  val readInput = spark.read.option("multiline", "true")
    .json("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/explodeFuncData.json")


  val explodedDF = readInput.select(
    col("business_id"),
    col("full_address"),
    explode(array(
      struct(lit("Monday").as("day"), col("hours.Monday.open").as("open_time"), col("hours.Monday.close").as("close_time")),
      struct(lit("Tuesday").as("day"), col("hours.Tuesday.open").as("open_time"), col("hours.Tuesday.close").as("close_time")),
      struct(lit("Wednesday").as("day"), col("hours.Wednesday.open").as("open_time"), col("hours.Wednesday.close").as("close_time")),
      struct(lit("Thursday").as("day"), col("hours.Thursday.open").as("open_time"), col("hours.Thursday.close").as("close_time")),
      struct(lit("Friday").as("day"), col("hours.Friday.open").as("open_time"), col("hours.Friday.close").as("close_time")),
      struct(lit("Saturday").as("day"), col("hours.Saturday.open").as("open_time"), col("hours.Saturday.close").as("close_time")),
      struct(lit("Sunday").as("day"), col("hours.Sunday.open").as("open_time"), col("hours.Sunday.close").as("close_time"))
    )).as("exploded")
  ).select(
    col("business_id"),
    col("full_address"),
    col("exploded.day"),
    col("exploded.open_time"),
    col("exploded.close_time")
  )

  explodedDF.show(truncate = false)
}
