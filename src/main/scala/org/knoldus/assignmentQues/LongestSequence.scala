package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object LongestSequence extends App {
  val spark = SparkSession.builder()
    .appName("Longest Sequence")
    .master("local[1]")
    .getOrCreate()

  val inputData = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/LongestSequence.csv")

  val convertedTypeDf = inputData.withColumn("time", col("time").cast("integer"))

  val addLagCol = convertedTypeDf.withColumn("lagCol", lag("time", 1)
    .over(Window.partitionBy("ID").orderBy("time")))

  val addFlagValue = addLagCol.withColumn("flag",
    when(col("time") - col("lagCol") === 1, 0).otherwise(1))

  val addSumCol = addFlagValue.withColumn("sum", sum("flag")
    .over(Window.partitionBy("ID").orderBy("time")))

  addSumCol.show(false)

  val countDF = addSumCol.groupBy("ID", "sum")
    .agg(count("sum").alias("time"))
  countDF.show(false)

  // Find the longest sequence of consecutive numbers for each ID
  val longestSequenceDF = countDF.groupBy("ID")
    .agg(max("time").alias("time"))

  // Show the result
  longestSequenceDF.show()
}
