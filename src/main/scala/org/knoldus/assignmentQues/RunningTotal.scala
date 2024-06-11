package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RunningTotal extends App {
val spark = SparkSession.builder()
  .appName("Running Total")
  .master("local[1]")
  .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/Running_Total.csv")

  inputDf.show(false)

  val windowSpec = Window.partitionBy("department").orderBy("items_sold").rowsBetween(Window.unboundedPreceding, Window.currentRow)


  val resultdf = inputDf.withColumn("running_total", sum("items_sold").over(windowSpec))
  resultdf.show(false)

 val running_total_Diff = resultdf.withColumn("diff", col("running_total") - lag(col("running_total"), 1, 0).over(Window.partitionBy("department").orderBy("items_sold")))
running_total_Diff.show(false)
}
