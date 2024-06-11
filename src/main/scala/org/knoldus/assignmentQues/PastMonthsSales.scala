package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PastMonthsSales extends App {
  val spark = SparkSession.builder()
    .appName("Occurence Of Months and Years(from 24 months)")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/24Months.csv")

  val groupedDf = df.groupBy(col("YEAR_MONTH")).agg(sum("AMOUNT").as("AMOUNT"))

  val datatoDateFormat = groupedDf.withColumn("dates", to_date(col("YEAR_MONTH"), "yyyyMM"))
  datatoDateFormat.show(false)

  val getstartDate = datatoDateFormat.select(trunc(date_sub(first("dates"), 24 * 30), "MM")
    .as("startDate")).first()
  val startDate = getstartDate.getAs(0).toString
  //println(startDate2)

  val rangeDf = spark.range(0, 25)

  val dfWithStartDate = rangeDf.withColumn("dates", lit(startDate))
  //dfWithNewColumn.show(false)

  val updatedDates = dfWithStartDate.withColumn("AllDates", add_months(col("dates"), col("id")))
    .drop(col("id"), col("dates"))
  updatedDates.show(false)

  val joinedDf = updatedDates.as("tb1").join(datatoDateFormat.as("tb2"), col("tb1.AllDates") === col("tb2.dates"), "left")
    .drop(col("tb2.dates"), col("tb2.YEAR_MONTH")).orderBy(col("AMOUNT").desc).na.fill(0)

  val result = joinedDf.withColumn("Year_Month",
    date_format(col("AllDates"), "yyyyMM")).drop("dates")

  result.show(25, false)
}
