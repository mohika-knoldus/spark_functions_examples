package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GapInSalaries extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Gap In Salary Of employees Based On Highest Paid Employee")
    .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/GapSalaries.csv")

  val windowSpec = Window.partitionBy("department").orderBy(col("salary").desc)

  val resultDf = inputDf.withColumn("max_salary", first("salary").over(windowSpec))
    .withColumn("diff", col("max_salary") - col("salary"))
    .drop("max_salary")
  resultDf.show(false)

}
