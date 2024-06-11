package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DifferenceBetweenRunningTotal extends App {
  val spark = SparkSession.builder()
    .appName("difference of running total")
    .master("local[1]")
    .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/Running_Total.csv")

  val windowSpec = Window.partitionBy("department").orderBy("items_sold").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val windowSpecDiff = Window.partitionBy("department").orderBy("items_sold")
  val result = inputDf.withColumn("running_total", sum("items_sold").over(windowSpec))
    .withColumn("diff", col("running_total") - lag(col("running_total"), 1, 0).over(windowSpecDiff))

  result.show(false)
}
