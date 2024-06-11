package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession

object FinalPrice extends App {
  val spark = SparkSession.builder()
    .appName("Practice Application")
    .master("local[1]")
    .getOrCreate()

  val realEstateRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/real_estate.txt/")

  val headerFromRdd = realEstateRdd.first()
  val dataWithoutHeader = realEstateRdd.filter(line => line != headerFromRdd)

  val finalRdd = dataWithoutHeader.map(row => {
  val columns = row.split("\\|")
    val propertyId = columns(0)
    val location = columns(1)
    val size = columns(5).toDouble
    val priceSQ = columns(6).toDouble
    val finalPrice = size * priceSQ
    s"$propertyId|$location|$finalPrice"
  })
  finalRdd.saveAsTextFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/output_files/finalPriceRdd")
}
