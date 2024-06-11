package org.knoldus.interviewQuestions

import org.apache.spark.sql.SparkSession

object HandlingCorruptRecord extends App {
  val spark = SparkSession.builder()
    .appName("Handling Corrupt data")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("mode" -> "dropMalFormed", "inferSchema" -> "true", "header" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/interviewQuestions/corruptRecords.csv")

  df.show(false)
}
