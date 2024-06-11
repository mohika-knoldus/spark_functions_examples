package org.knoldus.RDDBasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RddPractice extends App {
  val spark = SparkSession.builder()
    .appName("Practice Application")
    .master("local[1]")
    .getOrCreate()

  //val sc = new SparkContext(sparkConf)

  //  val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
  //  val arrayRDD = sparkSession.sparkContext.parallelize(array, 2)
  //  println("No. of elements in RDD:",arrayRDD.count())
  //  arrayRDD.foreach(println)
  //
  val fileRDD = spark.sparkContext.textFile("/home/knoldus/sampleTSV.tsv", 5)
  println("No. of rows in file: " + fileRDD.count())
  println("First row of file RDD: " + fileRDD.first())
  // println(fileRDD.collect())

  val employeeDataRDD = spark.sparkContext.textFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/employeeData.csv", 5)
  val headerOfEmployeeData = employeeDataRDD.first()
  val employeeDataWithoutHeader = employeeDataRDD.filter(line => line != headerOfEmployeeData)
  val allAges = employeeDataWithoutHeader.map(line => line.split(",")(1).toInt)
  val sumOfAllAges = allAges.sum()
  val numberOfEmployees = allAges.count()
  val averageAge = sumOfAllAges.toInt / numberOfEmployees.toInt
  println(averageAge)

  val listData = List(1, 2, 3, 4, 5)
  // the type of listRdd is RDD[Int]
  val listRdd = spark.sparkContext.parallelize(listData)
  listRdd.foreach(println)


}
