package org.knoldus.interviewQuestions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object singleLineToDf  extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("single line data to dataframe")
    .getOrCreate()

  import spark.implicits._

  val df = spark.read.
    csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/interviewQuestions/SingleLineFile.csv")

  val transformDf = df.withColumn("transformedData", regexp_replace(col("_c0"), "(.*?\\|){5}", "$0-"))
  val explodedDf = transformDf.withColumn("explodedData", explode(split(col("transformedData"), "\\|-")))
    .select("explodedData")

  explodedDf.show(false)

  val resultRdd = explodedDf.select(col("explodedData")).rdd.map(value => value.getString(0))
    .map(value => {
    val Array(name, branch, rollNo, field, contact) = value.split("\\|")
      (name, branch, rollNo, field, contact)
    })
  resultRdd.foreach(println)
  val resultDf = resultRdd.toDF("Name", "Branch", "RollNumber","Field","Contact")
  resultDf.show(false)

}
