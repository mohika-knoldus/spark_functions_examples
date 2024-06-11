package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, first, last}

object MergeRows extends App {
  val spark = SparkSession.builder()
    .appName("merger rows")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    ("100", "John", Some(35), None),
    ("100", "John", None, Some("Georgia")),
    ("101", "Mike", Some(25), None),
    ("101", "Mike", None, Some("New York")),
    ("103", "Mary", Some(22), None),
    ("103", "Mary", None, Some("Texas")),
    ("104", "Smith", Some(25), None),
    ("105", "Jake", None, Some("Florida"))).toDF("id", "name", "age", "city")

  val result = input.groupBy(col("id"), col("name")).agg(
    coalesce(first("age"), last("age")).as("age"),
    coalesce(first("city"), last("city")).as("city")
  )
  result.show(false)
}
