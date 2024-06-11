package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HighestPopulatedCity extends App {
  val spark = SparkSession.builder()
    .appName("Higly Populated City")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/populationData.csv")
  df.show(false)

  val correctedPopulation = df.withColumn("PopulationW", regexp_replace(col("population")," ", "").cast("Long"))
  correctedPopulation.show(false)

 val maxPopulation =  correctedPopulation.groupBy("country").agg(max("PopulationW").as("population"))
  maxPopulation.show(false)

 val maximumPopDf =  correctedPopulation.as("popData").join(maxPopulation.as("max"), col("popData.PopulationW") === col("max.population") && col("popData.country") === col("max.country"), "inner")
    .select(col("popData.name"), col("max.country"), col("max.population"))
  maximumPopDf.show(false)

  val resultDf = correctedPopulation.as("tb1").join(maximumPopDf.as("tb2"),col("tb1.name") === col("tb2.name"), "inner")
    .select(col("tb2.name"), col("tb2.country"), col("tb1.population"))

  resultDf.show(false)

}
