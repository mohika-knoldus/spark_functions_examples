package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object NonNullValuePerGroup extends App {
  val spark = SparkSession.builder()
    .appName("NON NULL VALUE PER GROUP")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._
  val data = Seq(
    (None, 0),
    (None, 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

  val resultDf = data.groupBy("group").agg(first("id", true).as("firstNon_null"))
    .orderBy("group")

  resultDf.show(false)


}
