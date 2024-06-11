package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PracticeOnnTransactionData extends App {
  val spark = SparkSession.builder()
    .appName("transaction analysis")
    .master("local[1]")
    .getOrCreate()

  val transactionDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/TransactionDetails.csv")

//  What is the total revenue generated from all sales transactions ?
  val revenue = transactionDf.groupBy(col("product_id"))
    .agg(sum("quantity").as("Groupedquantity"), sum("price").as("Groupedprice"))
    .withColumn("totalRevenue", expr("Groupedprice * Groupedquantity"))
    .show(false)

  transactionDf.printSchema()
//  How many unique customers made purchases ?
  println(transactionDf.select("customer_id").distinct().count())

//  Which product has the highest quantity sold ?

  transactionDf.groupBy("product_id")
    .agg(sum("quantity").as("total_quantity"))
    .select("product_id").where("")

//    Find the average price of all products.
//    Which customer made the highest total purchase amount ?
//  What is the maximum and minimum price of products ?
//    How many sales transactions occurred on each day of the week ?
//    Which product had the highest revenue ?
//  Find the total revenue generated for each customer
//.
//  Calculate the total revenue for each month
//.
//  How many transactions were made for each product ?
//  Determine the top 5 customers with the most transactions.
//    Find the average quantity of products sold per transaction.
//    Calculate the total revenue for each product category.
//    Which day of the week had the highest sales revenue ?

}
