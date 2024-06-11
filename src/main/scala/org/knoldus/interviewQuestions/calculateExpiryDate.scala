package org.knoldus.interviewQuestions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object calculateExpiryDate extends App {
  val spark = SparkSession.builder()
    .appName("Calculate Expiry Date")
    .master("local[1]")
    .getOrCreate()

  val inputDf = spark.read.options(Map("inferSchema" -> "true", "header" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/interviewQuestions/expiryDate.csv")

  val result = inputDf.withColumn("expiryDate", date_add(date_format(to_date(col("RechargeDate"), "yyyyMMdd"), "yyyy-MM-dd"), col("Remaining_days")))
  result.show(false)

}
