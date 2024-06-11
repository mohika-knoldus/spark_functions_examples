package org.knoldus.interviewQuestions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object InterviewQuestionsAzarudeen extends App {
val spark = SparkSession.builder()
  .master("local[1]")
  .appName("reading ~| separated file")
  .getOrCreate()

  val data = spark.read.text("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/PIpeTildSeparatedFile.csv")
 data.show(false)

 val header = data.first().mkString
 println(header)

 //  \\s	It is utilized to match white spaces which are analogous to [\t\n\r\f].
 val columnNames = header.split("~\\|(?!\\s)")
 columnNames.foreach(println)

 import spark.implicits._
// val schema = StructType(columnNames.map(valueString => StructField(valueString,StringType, true)))
 val dfWithoutHeader = data.filter(value => value.mkString != header).rdd.map(value => value.getString(0))
   .map(value => {
    val Array(name, age) = value.split("~\\|(?!\\s)")
    (name,age)
   }).toDF(columnNames: _*)
   dfWithoutHeader.show(false)



}
