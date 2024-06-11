package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExamReport extends App {
  val spark = SparkSession.builder()
    .appName("Exam Assessment Report")
    .master("local[1]")
    .getOrCreate()

  val inputDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/ExamAnswersData.csv")

  val result = inputDf.withColumn("QidCol", concat(lit("Qid_"), col("Qid")))
    .groupBy("Assessment", "ParticipantID", "GeoTag").pivot("QidCol")
    .agg(first("AnswerText"))
    .select(col("ParticipantID"), col("Assessment"), col("GeoTag"),
      col("Qid_1"), col("Qid_2"), col("Qid_3"))

  result.show(false)
}
