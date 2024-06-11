package org.knoldus.practice_questions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataframePractice extends App {
  val spark = SparkSession.builder()
    .appName("practice dataframes")
    .master("local")
    .getOrCreate()

  val schema = StructType(Seq(StructField("name", StringType, false),
    StructField("age", IntegerType, false),
    StructField("ascre", ArrayType(StringType, true), false)))

  val data = Seq(Row("cbhdc", 34, Array("wefce", "fefref")), Row("cervre", 45, Array("FCr3f", "Wfrf", "vreva")))

  val dataRdd = spark.sparkContext.parallelize(data)
  val textRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/employeeData.csv")

  import spark.implicits._

  val dataTwo = Seq(("acd", 3), ("CEd", 4), ("FCE", 5))
  val schemaT = Seq("name", "id")
  val dftwo = dataTwo.toDF(schemaT: _*)
  val df = spark.createDataFrame(dataRdd, schema)

//create df from rdd
  val practicedf = spark.createDataFrame(textRdd.map(value => Row(value)), schema)

  val columns = Seq("SNo", "Quote")
  val quoteData = Seq(("1", "success is not the key to happiness."),
    ("2","happiness is the key to success."),
    ("3","if you love what you are doing, you will be successful."))

  val quoteDF = quoteData.toDF(columns: _*)

  val convertCase = (strQuote: String) => {
    val arr = strQuote.split(" ")
    arr.map(f => f.substring(0, 1).toUpperCase + f.substring(1, f.length).mkString)
  }

  val methodToUDF = udf(convertCase) //registering the function as udf

  quoteDF.select(col("SNo"), methodToUDF(col("Quote"))
    .as("Quote")).show(false)
}
