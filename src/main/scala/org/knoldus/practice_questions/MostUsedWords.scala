package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession

object MostUsedWords extends App {
  val spark = SparkSession.builder()
    .appName("dee")
    .master("local[1]")
    .getOrCreate()

  val textRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/sample.txt/")
  val wordRdd = textRdd.flatMap(line => line.split(" "))
  val mappedRdd = wordRdd.map(word => (word,1))
  val reducedRdd = mappedRdd.reduceByKey(_ + _)
  val swappedRdd = reducedRdd.map(pair => (pair._2,pair._1))
  val sortedRdd = swappedRdd.sortByKey(ascending = false)
  println(sortedRdd.take(5).mkString)


}
