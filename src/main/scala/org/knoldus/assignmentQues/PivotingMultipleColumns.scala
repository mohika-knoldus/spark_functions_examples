package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotingMultipleColumns extends App {
  val spark = SparkSession.builder()
    .appName("cost average")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (100, 1, 23, 10),
    (100, 2, 45, 11),
    (100, 3, 67, 12),
    (100, 4, 78, 13),
    (101, 1, 23, 10),
    (101, 2, 45, 13),
    (101, 3, 67, 14),
    (101, 4, 78, 15),
    (102, 1, 23, 10),
    (102, 2, 45, 11),
    (102, 3, 67, 16),
    (102, 4, 78, 18)).toDF("id", "day", "price", "units")

  val result = data.groupBy("id").pivot("day")
    .agg(sum("price").as("price"),
      sum("units").as("units"))
    .select(col("id"), col("1_price").as("price_1"),
      col("2_price").as("price_2"),
      col("3_price").as("price_3"),
      col("4_price").as("price_4"),
      col("1_units").as("units_1"),
      col("2_units").as("units_2"),
      col("3_units").as("units_3"),
      col("4_units").as("units_4"))

  result.show(false)

  // Second Way
  val priceDf = data.withColumn("tempCOl", concat(lit("price_"), col("day")))
    .groupBy("id").pivot("tempCol")
    .agg(sum("price"))

  val unitDf = data.withColumn("tempCol", concat(lit("units_"), col("day")))
    .groupBy("id").pivot("tempCol")
    .agg(sum("units"))


  val resultDf = priceDf.join(unitDf, "id", "inner")
  //    .drop(unitDf("id"))

  resultDf.show(false)


}
