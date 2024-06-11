package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SplitColumnValues extends App {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Split by delimiter")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/dekimiterData.csv")
  df.printSchema()

  val col_split = (value: String, delimiter: String) => {
    value.split(delimiter.charAt(0))
  }
  val udf_split = udf(col_split)

  val resultDf = df.withColumn("valuessplit", udf_split(df.col("Values"), df.col("Delimiter")))
  resultDf.show(false)

  val delimiterRegex = df.select("Delimiter").collect().map(row => row.getString(0).mkString(",").replaceAll(",", "|"))
  println(delimiterRegex)
  val resultDF = df.withColumn("split_Values", split(col("Delimiter"), s"[$delimiterRegex]"))
  resultDF.show(false)

}
