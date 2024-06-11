package org.knoldus.sparkSessionEg

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object settingActiveSession extends App {

  val spark = SparkSession.builder()
    .appName("setting active session")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val dfOne = Seq((1, 1), (2, 2),  (3, 3)).toDF("value1", "value2")

  val sparkSessionTwo = spark.newSession()

  println(s"first spark session $spark")
  println(s"second spark session $sparkSessionTwo")

  println(s"spark session before setting active session:- ${SparkSession.active}")
  SparkSession.setActiveSession(sparkSessionTwo)
  println(s"spark session after setting active session:- ${SparkSession.active}")
  SparkSession.clearActiveSession()
  println(s"Spark session after clearing active session ${SparkSession.active}")

  println(s"default session before changing:- ${SparkSession.getDefaultSession}")
  SparkSession.setDefaultSession(sparkSessionTwo)
  println(s"default session after changing:- ${SparkSession.getDefaultSession}")

  println(spark.version)

  spark.udf.register("adding", (firstValue: Int, secondValue: Int) => firstValue + secondValue)

    val sumDf = dfOne.withColumn("sum", expr("adding(value1, value2)"))
    sumDf.show()

  val emptyDF = spark.emptyDataFrame
  emptyDF.show()

  case class Person(name: String, age: Int)

  val personsData = spark.sparkContext.parallelize(Seq(
    Person("Bob", 20),
    Person("Alice", 25),
    Person("Julie", 22)
  ))
  implicit val personEncoder = Encoders.product[Person]

  val personDS = spark.createDataset(personsData)

  val personDataset = spark.createDataset(personsData)
  val personDf = spark.createDataFrame(personsData)
  personDf.show()

  val rangeDs = spark.range(10)
  rangeDs.show()
  val anotherRangeDs = spark.range(5, 11)
  anotherRangeDs.show()

  val rangeWIthDefinedStepValue = spark.range(0, 21, 2)
  rangeWIthDefinedStepValue.show()

  val rangeWithPartitons = spark.range(0, 21, 2, 4)

  dfOne.createOrReplaceTempView("tableView")
  val tableToDf = spark.table("tableView")
  tableToDf.show()


  personDf.createOrReplaceTempView("people")
  val useOfSql = spark.sql("SELECT * FROM people WHERE age > :minimumAge", Map("minimumAge" -> 22))
  useOfSql.show()

  spark.stop
}
