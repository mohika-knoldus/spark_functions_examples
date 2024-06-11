package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BestSellerPerGenre extends App {
  val spark = SparkSession.builder()
    .appName("Bestsellers Per Genre")
    .master("local[1]")
    .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/BestSellersData.csv")

  val windowSpec = Window.partitionBy("genre").orderBy(col("quantity").desc)

  val resultDf = inputDf.withColumn("rank", rank.over(windowSpec))
    .select("*").where(col("rank") === 1 || col("rank") === 2)
  resultDf.show(false)



}
