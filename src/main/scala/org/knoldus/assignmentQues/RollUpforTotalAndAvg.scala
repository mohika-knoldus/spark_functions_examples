package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RollUpforTotalAndAvg extends App {

  val spark = SparkSession.builder()
    .appName("roll up use for depatment and company wise total nd average salary")
    .master("local[1]")
    .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/rollupFunc.csv")

  inputDf.rollup("department").agg(
  sum("salary").as("sum"),
    avg("salary").as("avg")).sort(col("sum").desc)
    .show(false)

}
