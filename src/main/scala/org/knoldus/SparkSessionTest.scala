package org.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

object SparkSessionTest extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

    println("First SparkContext:")
    println("APP Name :"+spark.sparkContext.appName);
    println("Deploy Mode :"+spark.sparkContext.deployMode);
    println("Master :"+spark.sparkContext.master);

    val sparkSession2 = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample-test")
      .getOrCreate();

    println("Second SparkContext:")
    println("APP Name :"+sparkSession2.sparkContext.appName);
    println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
    println("Master :"+sparkSession2.sparkContext.master);

  val df = spark.createDataFrame(List(("Scala", 25000), ("Spark", 35000), ("Php", 21000)))
  df.show()

  df.createOrReplaceTempView("sample_table")
  val df2 = spark.sql("SELECT _1 FROM sample_table")
  df2.show()

//  spark.table("sample_table").write.saveAsTable("sample_hive")
//  val df3 = spark.sql("SELECT _1,_2 FROM sample_hive")
//  df3.show()

  val ds = spark.catalog.listDatabases()
  ds.show()

  val ds2 = spark.catalog.listTables()
  ds2.show()

  val rdd = spark.sparkContext.range(1, 5)
  rdd.collect.foreach(println)

  val rdd2 = spark.sparkContext.textFile("/src/main/resources/text/alice.txt")
//  spark.sparkContext.stop()

  val sparkConf = new SparkConf().setAppName("sparkbyexamples.com").setMaster("local[1]")
  //val sparkContext = new SparkContext(sparkConf)

  val sc = SparkContext.getOrCreate(sparkConf)
  println(sc)

  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rddByParallelize = spark.sparkContext.parallelize(dataSeq, 2)
  println(rddByParallelize.getNumPartitions)
  }

