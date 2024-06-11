package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DatesDiff extends App {

  val spark = SparkSession.builder()
    .appName("difference between dates")
    .master("local[1]")
    .getOrCreate()
  import spark.implicits._

  val datesDf = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  datesDf.show(false)
//  val dateSplit = datesDf.withColumn("idk", split(col("date_string"), "/"))
//  dateSplit.printSchema()
//  dateSplit.show(false)

  val dateComposer = (dateString: String) => {
    val dateArr = dateString.split("/")
    s"${dateArr(2)}-${dateArr(1)}-${dateArr(0)}"
  }

  val formatDate = udf(dateComposer)

  datesDf.withColumn("to_date", formatDate(col("date_string")))
    .withColumn("diff", datediff(current_date(), col("to_date")))
    .show(false)

  val dateFormat = datesDf.withColumn("to_Date",date_format(to_date(col("date_string"), "dd/MM/yyyy"), "yyyy-MM-dd"))
  val result = dateFormat.withColumn("diff", datediff(current_date(), col("to_Date")))
  result.show(false)
  dateFormat.show()
}
