package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object pivotColumnToRow extends App {
  val spark = SparkSession.builder()
    .appName("Pivot Column To Row")
    .master("local[1]")
    .enableHiveSupport()
    .getOrCreate()

  val sparkSecond = spark.newSession()

  println(spark.version)

  val schema = StructType(Seq(
    StructField("udate", StringType, true),
    StructField("cc", StringType, true)
  ))


  val df = spark.read.schema(schema)
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/PivotColumnData.csv")

  df.show(false)
  df.printSchema()

  val ab = df.groupBy().pivot(col("udate")).agg(first("cc"))
//  ab.printSchema()
  ab.show(false)

}
