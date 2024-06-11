package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{add_months, col, current_date, current_timestamp, date_add, date_format, date_sub, datediff, dayofmonth, dayofweek, dayofyear, from_unixtime, hour, last_day, localtimestamp, make_date, minute, month, next_day, quarter, second, to_date, to_timestamp, trunc, unix_timestamp, weekofyear, year}

object DateAndTimePractice extends App {
  val spark = SparkSession.builder()
    .appName("Date And Time Functions")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/dateAndTimeData.csv")

//  import spark.implicits._

  val dateAndTimeDf = df.withColumn("epochTimestamp", unix_timestamp())
    .withColumn("currentDate", current_date())
    .withColumn("currentTimestamp", current_timestamp())
    .drop("date", "time")

  dateAndTimeDf.show(false)

  dateAndTimeDf.select(col("id"), col("name"),
    from_unixtime(col("epochTimestamp"), "yyyy-MM-dd").as("epochToDate"),
    unix_timestamp(col("currentTimestamp")).as("timestampToEpoch"),
    to_timestamp(col("epochTimestamp")).as("epochToTimestamp")
    .as("epochToTimestamp")).show(false)

  val addMonthDf = dateAndTimeDf.withColumn("addedMonth", add_months(col("currentDate"), 3))
    .show(false)

  dateAndTimeDf.withColumn("timestampWithoutTimeZone", localtimestamp()).show(false)
  dateAndTimeDf.withColumn("dateFormatFromtimestamp", date_format(col("currentTimestamp"), "dd-MM-yy")).show(false)
  dateAndTimeDf.withColumn("date_after_N_days", date_add(col("currentDate"), 22)).show(false)
  dateAndTimeDf.withColumn("date_before_N_days", date_sub(col("currentDate"), 11)).show(false)
  dateAndTimeDf.withColumn("date_difference", datediff(col("currentDate"), col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("extracted_year", year(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("quarter", quarter(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("month", month(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("dayOfWeek", dayofweek(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("dayOfYear", dayofyear(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("dayOfMonth", dayofmonth(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("hours", hour(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("lastDayOfMonth", last_day(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("minutes", minute(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("firstdayAfter", next_day(col("currentDate"), "Sun")).show(false)
  dateAndTimeDf.withColumn("Seconds", second(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("weekOfYear", weekofyear(col("currentDate"))).show(false)
  dateAndTimeDf.withColumn("dateConvert", to_date(col("currentTimestamp"))).show(false)
  dateAndTimeDf.withColumn("truncate", trunc(col("currentDate"), "year")).show(false)
  dateAndTimeDf.withColumn("truncate", trunc(col("currentDate"), "mon")).show(false)


  dateAndTimeDf.withColumn("epochTotimestamp",col("epochTimestamp").cast("Timestamp"))
    .withColumn("month", month(col("epochTotimestamp"))).show(false)

//  val dataToAdd = Seq((100) ,(20) ,(3) , (44), (22))
//  val dataToAddDf = dataToAdd.toDF("daysNumber")
//  dataToAddDf.show(false)
//
//  dateAndTimeDf.unionByName(dataToAddDf).show(false)

}
