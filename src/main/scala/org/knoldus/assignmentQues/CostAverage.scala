package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CostAverage extends App {
  val spark = SparkSession.builder()
    .appName("cost average")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._
  val data = Seq(
    (0, "A", 223, "201603", "PORT"),
    (0, "A", 22, "201602", "PORT"),
    (0, "A", 422, "201601", "DOCK"),
    (1, "B", 3213, "201602", "DOCK"),
    (1, "B", 3213, "201601", "PORT"),
    (2, "C", 2321, "201601", "DOCK")
  ).toDF("id", "type", "cost", "date", "ship")

  val result = data.groupBy(col("id"), col("type")).pivot("date").agg(avg("cost"))
    .orderBy("id", "type")

  result.show(false)

  val resultPartTwo = data.groupBy(col("id"), col("type")).pivot("date").agg(collect_list("ship"))
    .orderBy("id", "type")
   resultPartTwo.show(false)
}
