package org.knoldus.practice_questions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object DataFrameQuestions extends App {

  val spark = SparkSession.builder()
    .appName("DATAFRAME PRACTICE QUESTIONS")
    .master("local[1]")
    .getOrCreate()

  val  sampleDf = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/questionsData.csv")


  val sampleDff = spark.read.option("header", "true").option("inferSchema", "true").format("csv")
    .load("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/questionsData.csv")

  sampleDff.show(false)

  //  Write a Spark code snippet to calculate the total count of records in a given DataFrame.

  println(sampleDf.count())

  sampleDf.select(count("Age").as("No. of records")).show()

  //  Given a DataFrame with columns "name" and "age", write Spark code to filter out all the records where the age is greater than 30.

  sampleDf.filter("age > 30").show(false)

  sampleDf.where(sampleDf("age") > 30).show(false)

  //  Write a Spark program to calculate the average value of a specific numeric column in a DataFrame.
  sampleDf.agg(avg("Age").as("Average age")).show()

  sampleDf.select(avg("Age") as ("AverageAge")).show()

  //  Given a DataFrame containing sales data with columns "product" and "quantity",
  //  write Spark code to find the product with the highest quantity sold.

  val groupedDf = sampleDf.groupBy("product").agg(sum("quantity").as("totalSold"))
  groupedDf.orderBy(col("totalSold").desc).limit(1).show(false)

  //  val windowF = Window.partitionBy("product")
  //  sampleDf.withColumn("quantitySold", sum("quantity").over(windowF)).orderBy(col("quantitySold").desc).show(false)

  //  Write a Spark program to join two DataFrames based on a common column and display the combined result.

  val dfOne = spark.read.option("header", "true").option("inferSchema", true)
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/Emp_Details.csv")

  val dfTwo = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/Emp_Country.csv")

  dfOne.join(dfTwo, dfOne("id") === dfTwo("id"), "fullOuter").show(false)

  //  Given a DataFrame with columns "date" and "revenue",
  //  write Spark code to calculate the total revenue earned for each month.

  val revenueDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/revenue.csv")

  val monthWiseDf = revenueDf.withColumn("month", month(col("date")))

  monthWiseDf.groupBy(col("month").as("month"))
    .agg(sum("revenue").as("total_revenue")).show(false)

  //  Write a Spark program to load a JSON file and extract specific fields to create a new DataFrame.
  val jsonDf = spark.read.option("multiline", "true")
    .json("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/sampleJSON.json")
  jsonDf.printSchema()

  val extractedDf = jsonDf.select("field1", "field2", "field3")
  extractedDf.show(false)

  //  Given a DataFrame containing customer information with columns "name", "age", and "country",
  //  write Spark code to find the most common country among the customers.

  val countryCountDf = sampleDf.groupBy("country").agg(count("*").as("countryCount"))
    .orderBy(desc("countryCount"))
    .limit(1)
    .select("country")
  countryCountDf.show(false)

  //  Write a Spark program to read data from a CSV file and convert it into Parquet format.

//    val dataCSV = spark.read.option("header", "true").format("csv")
//      .load("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/employeeData.csv")
//     dataCSV.write.partitionBy()format("parquet")
//       .save("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/org/knoldus/output_files/parquetFiles")

  //  Given a DataFrame with columns "product", "price", and "quantity",
  //  write Spark code to calculate the total sales value (price * quantity) for each product

  //  sampleDf.groupBy("product").agg(sum("quantity").as("quantity"))
  //    .withColumn("price", col("price"))
  //    .selectExpr("price * quantity")
  //    .show(false)

  sampleDf.withColumn("totalSales", col("price") * col("quantity"))
    .groupBy("product").agg(sum("totalSales").as("totalSales")).show(false)

  sampleDf.drop(col("name"), col("age")).show(false)

  sampleDf.filter((col("age") >= 30) and (col("quantity") <= 20)).show(false)


  //lit() if the specified column in withColumn("___name__") is present in the table,
  // then it replaces all the vales of that column to literal passed in lit("literal")
  //but if the column is not present in the table then it creates a new column of
  // that name and assigns the value passed in lit("literal method") to all rows.
  sampleDf.withColumn("quantitycec", lit(1)).show(false)


//split or separate a column into two columns using split function
  sampleDf.withColumn("ProductC", split(col("product"), "_").getItem(0))
    .withColumn("productS", split(col("product"), "_").getItem(1))
    .drop("product")
    .show(false)

  sampleDf.select(split(col("product"),"_").as("arrayProduct")).show(false)
  println(countryCountDf.queryExecution.logical)
  println(sampleDf.queryExecution.optimizedPlan)
  println(sampleDf.queryExecution.commandExecuted)
  countryCountDf.explain(true)

 println(sampleDf.first().getAs[Int]("product"))
//  sampleDf.describe()
//  sampleDf.dropDuplicates().show()
//  sampleDf.na.drop()
//  sampleDf.withColumnRenamed("existing", "new")
//  sampleDf.withColumn("product", upper(col("product")))

  val df = spark.createDataFrame(Seq((1, Array("apple", "banana", "cherry")), (2, Array("orange", "grape"))))
    .toDF("id", "fruits")
  df.show(false)

  val explodedDF = df.select(col("id"), explode(col("fruits")).as("fruit"))

  explodedDF.show()

//  sampleDf.union()




}
