package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ConvertStringsArrayToString extends App {
  val spark = SparkSession.builder()
    .appName("conversion of array of strings to single string")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._
  val words = Seq(Array("hello", "world")).toDF("words")
  val result = words.withColumn("solution", concat_ws(" ", col("words")))

  result.show(false)
}
