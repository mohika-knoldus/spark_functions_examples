package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PerentRank extends App {
  val spark = SparkSession.builder()
    .appName("Rank based on salary percentage")
    .master("local[1]")
    .getOrCreate()

  val salaryDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/PercentRankData.csv")

  val result = salaryDf.withColumn("percentage", percent_rank().over(Window.orderBy(col("Salary"))))
    .withColumn("percentage",when(col("percentage") >= 0.6, "High")
      .when(col("percentage") < 0.5, "Low")
      .otherwise("Average"))
    .sort(col("salary").desc)
  result.show(false)

}
