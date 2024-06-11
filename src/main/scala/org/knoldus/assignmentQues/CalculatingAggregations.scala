package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CalculatingAggregations extends App {
  val spark = SparkSession.builder()
    .appName("Calculating aggregations")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/calculateAgg.csv")


  val result = df.withColumn("population", regexp_replace(col("population"), " ", "").cast("Long"))
    val maximumPopulation = result.select(max("population").as("max_population"))
  maximumPopulation.show(false)

  val max_population = maximumPopulation.first().getAs(0).toString
  println(max_population)

  val cityWithMaxPopulation = result.select("name").where(col("population") === max_population)
  cityWithMaxPopulation.show(false)

}