package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MaxPerGroup extends App {
  val spark = SparkSession.builder()
    .appName("collect maximum value in group")
    .master("local[1]")
    .getOrCreate()

  val nums = spark.range(5).withColumn("group", col("id") % 2)
  nums.show(false)

  val result = nums.groupBy("group").agg(max("id").as("max_id"))
  println("Maximum id from Group")
  result.show(false)

  println("collected ids per group")
  val groupedData = nums.groupBy("group").agg(collect_list("id").as("ids"))
  groupedData.show(false)
  groupedData.printSchema()

  groupedData.select(col("group"), sort_array(col("ids"), false)).show(false)

  nums.groupBy("group").agg(max("id").as("max_id"),min("id").as("min_id")).show(false)
}
