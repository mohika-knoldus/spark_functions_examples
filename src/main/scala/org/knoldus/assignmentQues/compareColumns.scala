package org.knoldus.assignmentQues

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object compareColumns extends App{
val spark = SparkSession.builder()
  .appName("columns compare and fetch id")
  .master("local[1]")
  .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/compareColumns.csv")

  df.show(false)
  val explodeddf = df.withColumn("wordsArr", explode(split(col("words"), ",")))
 explodeddf.show(false)

  val tempResult = explodeddf.groupBy("wordsArr").agg(collect_list("id").as("ids"))
   tempResult.as("tb1").join(df.as("tb2"), tempResult("wordsArr") === df("word"))
     .select(col("wordsArr"), col("ids")).show(false)

//  val resultDF = explodeddf.groupBy("word")
//    .agg(collect_set(col("id")).as("ids"))

//  val resultDF = explodeddf.filter(col("word").contains(col("wordsArr")))
//  resultDF.show(false)


}
