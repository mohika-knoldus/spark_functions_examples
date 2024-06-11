package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession

object SumOfListElements extends App {
 val spark = SparkSession.builder()
   .appName("Sum Of List Elements")
   .master("local[1]")
   .getOrCreate()

 val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  val sumOfListElements = listRdd.reduce(_ + _)
  println(sumOfListElements)

}
