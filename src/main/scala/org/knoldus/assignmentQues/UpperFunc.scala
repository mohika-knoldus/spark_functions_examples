package org.knoldus.assignmentQues

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object UpperFunc extends App {
  val spark = SparkSession.builder()
    .appName("Add Aggregation To Column")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._
val df = spark.read.options(Map("header" -> "true", "indferSchema" -> "true"))
  .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/UpperFunctionData.csv")

  val changeToUpper = (yourCol: String) => {
    yourCol.toUpperCase
  }
  val convertToUpper = udf(changeToUpper)
  df.withColumn("upper_city", convertToUpper(col("city"))).show(false)
}