package org.knoldus.sparkSessionEg

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType, MapType}

object ColumnsApi extends App{
  val spark = SparkSession.builder()
    .appName("examples for column api")
    .master("local[1]")
    .getOrCreate()
import spark.implicits._
  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/questionsData.csv")

//  val transformDf = df.withColumn("ad", col("country").cast())

// val result = df.select(-col("price"))
//  result.show()

  val ascendingNulls = df.sort(col("selling_price").asc_nulls_last).select("product", "selling_price")
  ascendingNulls.show()

  val countryInitials = df.select(col("country").substr(0,3))
  countryInitials.show()

  val containsCountry = df.select(col("country").contains("USA"))
  containsCountry.show()

val outOfStockItems = df.filter(!col("availability")).select("product", "price", "availability")
  outOfStockItems.show()

  val soldOnSamePrice = df.filter(col("price").equalTo(col("selling_price")))
    .select("product", "price", "availability", "selling_price")
  soldOnSamePrice.show()

  val soldOnDifferentPrice = df.filter(col("price").notEqual(col("selling_price")))
    .select("product", "price", "availability", "selling_price")
  soldOnDifferentPrice.show()

  val priceDf = df.filter(col("selling_price") >= 45)
    .select("product", "price", "availability", "selling_price")
  priceDf.show()

  val safeForNull = df.select(col("price"), col("selling_price"), col("price").eqNullSafe(col("selling_price")))
  safeForNull.show()

  val resetQuantity = df.select(col("quantity"), col("product"),
    when(col("quantity") <= 15, 20)
    .when(col("quantity") > 15 && col("quantity") <= 25 , col("quantity"))
      .otherwise(10).as("restocked"))

  resetQuantity.show()

  val useOfBetween = df.select(col("price"), col("price").between(50, 100).as("priceBetween"))
  useOfBetween.show

//  val result = df.select(col("selling_price"), col("selling_price").isNotNull.as("Is Not Null ?"))
//  result.show()

val result = df.select(col("price")).where(col("price") > 45 and col("price") < 100)
  result.show()

  val addition = df.select(col("price"), col("tax"), col("price") + col("tax"))
  addition.show()

  val finalPrice = df.select(col("price"),col("selling_price"), col("tax"), col("selling_price") - col("tax"))
  finalPrice.show()

  val multiplyDf = df.select(col("price").multiply(col("quantity")).as("sales"))
  multiplyDf.show()

  val pricedDf = df.select(col("price") % 2)
  pricedDf.show()

  val country = List("India", "USA", "UK", "Australia", "Afghanistan")
  val checkCountry = df.select(col("country"), col("country").isInCollection(country))
  checkCountry.show()

  val filteredDf = df.filter(col("product").like("%_apple"))
    .select("product", "price", "availability", "selling_price")
  filteredDf.show()
  filteredDf.explain(true)

//  val filteredDF = df.filter(col("product").rlike("^.*_apple$"))
//    .select("product", "price", "availability", "selling_price")
//  filteredDF.show()

  val filteredDF = df.filter(col("product").ilike("%apple%"))
     .select("product", "price", "availability", "selling_price")
  filteredDF.show()


  // Sample data as a DataFrame with an Array column and a Map column
  val data = Seq(
    (1, Array("apple", "banana", "orange")),
    (2, Array("grapes", "mango")),
    (3, Array("kiwi", "papaya", "pineapple")),
    (4, Array("strawberry", "watermelon"))
  )

  val dataDf = data.toDF("id", "fruits")

  val fruitsStartingWithA = dataDf.select(col("fruits").getItem(0).startsWith("p"))
  fruitsStartingWithA.show()

  val filterDf = dataDf.select("id", "fruits").filter($"fruits".getItem(2).endsWith("e"))
  filterDf.show()

  val renamedDf = dataDf.select(col("id").alias("number"), col("fruits"))
  renamedDf.show()


  val extractedFruits = dataDf.withColumn("first_fruit", col("fruits").getItem(0))
    .withColumn("second_fruit", col("fruits").getItem(1))
  extractedFruits.show()

  val friendData = Seq(Row("Alex", 20, Row("Bob",30)),
  Row("Cathy",40, Row("Doge", 40)))
  val friendRdd = spark.sparkContext.parallelize(friendData)

  val schema = StructType(Seq(StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("friend", StructType(Seq(StructField("friendName", StringType),
      StructField("friendAge", IntegerType)
    )))))
 val friendDf = spark.createDataFrame(friendRdd, schema)

  val changedDf = friendDf.select(col("friend").withField("friendName", lit("BOB")))
  changedDf.show()

  val updatedDf = friendDf.select(col("friend").dropFields("friendAge").as("friend"))
  updatedDf.show()

  val fetchField = friendDf.select(col("friend").getField("friendAge"))
  fetchField.show

 val inputSchema = StructType(Seq(
   StructField("id", IntegerType),
   StructField("namesAndAge", MapType(StringType,IntegerType))
 ))

  val inputData = Seq(Row(1, Map("Bob" -> 22, "Alice" -> 20)),
  Row(2, Map("Jenn" -> 23, "Zoey" -> 21))
  )

  val inputDf = spark.createDataFrame(spark.sparkContext.parallelize(inputData), inputSchema)
  inputDf.show(false)
  val resultDf = inputDf.select(col("id").name("S.NO").cast("Int"), explode(col("namesAndAge")).as(Seq("Name", "Age")))
  resultDf.show(false)

  val resultTwo = resultDf.sort(col("Age").desc)
 val dataFlow = Seq((1, 2), (1, 4), (3, 2))
  val dataDF = dataFlow.toDF("value1", "value2")

  val bitwiseORExample = dataDF.select(col("value1").bitwiseOR(col("value2")))
  bitwiseORExample.show()

  val bitwiseANDExample = dataDF.select(col("value1").bitwiseAND(col("value2")))
  bitwiseANDExample.show()

  val bitwiseXORExample = dataDF.select(col("value1").bitwiseXOR(col("value2")))
  bitwiseXORExample.show()
}



