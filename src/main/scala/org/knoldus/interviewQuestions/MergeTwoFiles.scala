package org.knoldus.interviewQuestions

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MergeTwoFiles extends App {
val spark = SparkSession.builder()
  .appName("Merge Two Files")
  .master("local[1]")
  .getOrCreate()

  import spark.implicits._
val fileOne = spark.read.options(Map("Delimiter" -> "|", "header" -> "true", "inferSchema" -> "true"))
  .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/interviewQuestions/mergeFile1.csv")
fileOne.show(false)

  val fileTwo = spark.read.options(Map("Delimiter" -> "|", "header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/interviewQuestions/mergeFile2.csv")
fileTwo.show(false)

  //val transformFileOne = fileOne.withColumn("Gender", lit(null))
  val mergedFiles = fileOne.unionByName(fileTwo, true)
  mergedFiles.show(false)




}
