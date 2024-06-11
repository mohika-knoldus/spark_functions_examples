package org.knoldus.assignmentQues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SalesAnalysis extends App {
  val spark = SparkSession.builder()
    .appName("Sales Analysis")
    .master("local[1]")
    .getOrCreate()

  val productDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/products.csv")

  val sellersDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/sellers.csv")

  val salesDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/sales.csv")

  val numberOfOrders = salesDf.select(countDistinct(col("order_id"))).head().mkString
  println(s"number of Orders -> $numberOfOrders")

  val numberOfProducts = productDf.select(count("product_id")).head().toString
  println(s"number of products -> $numberOfProducts")

  val numberOfSellers = sellersDf.select(countDistinct("seller_id")).head().mkString
  println(s"number of sellers -> $numberOfSellers")

  val productSoldAtLeastOnce = salesDf.select(countDistinct("product_id")).head().mkString
  println(s"products sold atleast once $productSoldAtLeastOnce")

  val productsInMostOrders = salesDf.groupBy("product_id").agg(countDistinct("order_id").as("orderCount")).orderBy(col("orderCount").desc)
    .select(first("product_id").as("product in most orders"))
  productsInMostOrders.show(false)

  val distinctProductSoldInDay = salesDf.groupBy("date").agg(countDistinct("product_id").as("distinct_products_sold"))
  distinctProductSoldInDay.show(false)

  val groupedSales = salesDf.groupBy("product_id").agg(sum("num_pieces_sold").as("totalSold"))
  val salesAndPrice = groupedSales.join(productDf, "product_id", "inner")
    .select((col("totalSold") * col("price")).as("revenue"))
    .select(avg("revenue").as("avg-revenue"))

  salesAndPrice.show(false)

  //avg % contibution
  val joinedDf = salesDf.join(sellersDf, "seller_id", "left")

  val contributionDf = joinedDf.withColumn("contribution", col("num_pieces_sold") / col("daily_target"))
  val averageContributionDF = contributionDf.groupBy("seller_id", "seller_name", "daily_target")
    .agg(avg("contribution").alias("average_contribution"))
  averageContributionDF.show(false)

  //second most selling and least selling seller
  val groupedDf = salesDf.groupBy("product_id", "seller_id").agg(sum("num_pieces_sold").as("totalSold"))
  groupedDf.show(false)
 val windowDesc = Window.partitionBy("product_id").orderBy(col("totalSold").desc)
  val windowAsc= Window.partitionBy("product_id").orderBy(col("totalSold"))

  val rankedDf = groupedDf.withColumn("rank_asc", dense_rank().over(windowAsc))
    .withColumn("rank_desc", dense_rank().over(windowDesc))
  rankedDf.show(false)

  val onlySoldOne = rankedDf.where(col("rank_asc") === col("rank_desc")).select(
    col("product_id").alias("single_seller_product_id"), col("seller_id").alias("single_seller_seller_id"),
    lit("Only seller or multiple sellers with the same results").alias("type"))

  onlySoldOne.show(false)

  val secondMostSelling = rankedDf.where(col("rank_desc") === 2).select(
    col("product_id").alias("second_seller_product_id"), col("seller_id").alias("second_seller_seller_id"),
    lit("Second top seller").alias("type"))
  secondMostSelling.show(false)

  val leastSelling = rankedDf.where(col("rank_asc") === 1)
    .select("product_id", "seller_id")
  leastSelling.show(false)

}