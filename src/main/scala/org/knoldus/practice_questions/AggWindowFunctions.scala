package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AggWindowFunctions extends App {
  val spark = SparkSession.builder()
    .appName("Querying with dataframe")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/EmpSalary.csv")
  df.show
  df.printSchema()

  val df2 = df.select(array(df.columns.map(col): _*))
  df2.printSchema

  val dfGrouped = df.groupBy(col("Emp Department"))
    .agg(collect_list("Employee Name") as ("Emp Name"))

  dfGrouped.show(false)

  val array_contains_df = dfGrouped.withColumn("result", array_contains(col("Emp Name"), "Alice Johnson"))

  array_contains_df.show(false)

  val array_appended_df = dfGrouped.select(array_append(col("Emp Name"), null))
  array_appended_df.show(false)

  val compact_array = array_appended_df.select(array_compact(col("array_append(Emp Name, null)")))
  compact_array.show(false)

  val array_distinct_append = dfGrouped.select(array_append(col("Emp Name"), "John Smith") as ("Emp Name"))
  array_distinct_append.show(false)

  val array_distinct_df = array_distinct_append.select(array_distinct(col("Emp Name")))
  array_distinct_df.show(false)

  val array_element_at_df = dfGrouped.select(element_at(col("Emp Name"), -1))
  array_element_at_df.show(false)

  val approx_distinct = df.select(approx_count_distinct("Emp Salary"))
  approx_distinct.show(false)

  val averageSalary = df.select(avg("Emp Salary"))
  averageSalary.show

  val allSalaries = df.select(collect_list(col("Emp Salary")))
  allSalaries.show(false)

  val allDistinctSalaries = df.select(collect_set("Emp Salary"))
  allDistinctSalaries.show(false)

  val count_Distinct_Salaries = df.select(countDistinct(col("Emp Salary")))
  count_Distinct_Salaries.show(false)

  val count_Salary = df.select(count(col("Emp Salary")))
  count_Salary.show(false)

  val firstValue = df.select(first(col("Emp Salary")))
  firstValue.show()

  val lastValue = df.select(last(col("Emp Salary")))
  lastValue.show()

  val sumAll = df.select(sum("Emp Salary"))
  sumAll.show()

  val sumDistinctValues = df.select(sum_distinct(col("Emp Salary")))
  sumDistinctValues.show()

  //average salaries per department


  val dfWithNumericSalary = df.withColumn("Emp Salary", col("Emp Salary").cast("double"))
  val avgSalaryDepartmentWise = dfWithNumericSalary.groupBy(col("Emp Department")).avg("Emp Salary")
  avgSalaryDepartmentWise.show(false)

  //group by min sal per department

  val minSalaryDepartmentWise = dfWithNumericSalary.groupBy(col("Emp Department")).min("Emp Salary")
  minSalaryDepartmentWise.show(false)

  //max
  val maxSalaryDepartment = dfWithNumericSalary.groupBy("Emp Department").max("Emp Salary")
  maxSalaryDepartment.show(false)

  //sum
  val sumOfSalariesPerDepartment = dfWithNumericSalary.groupBy("Emp Department").sum("Emp Salary")
  sumOfSalariesPerDepartment.show(false)

  val aggFunc = dfWithNumericSalary.groupBy("Emp Department")
    .agg(
      sum("Emp Salary") as ("sum salary"),
      avg("Emp Salary") as ("average salary"),
      min("Emp Salary") as ("minimum salary"),
      max("Emp Salary") as ("maximum salary")
    )
  aggFunc.show(false)

  //using condition
  dfWithNumericSalary.groupBy("Emp Department")
    .agg(
      sum("Emp Salary") as ("sum salary"),
      avg("Emp Salary") as ("average salary"),
      min("Emp Salary") as ("minimum salary"),
      max("Emp Salary") as ("maximum salary")
    )
    .where(col("average salary") > 45000)
    .show(false)

  //Window Functions

  val windowSpec = Window.partitionBy("Emp Department").orderBy("Emp Salary")
  df.withColumn("row_number", row_number.over(windowSpec))
    .show(false)

  //ordering in descending
  val windowSpec2 = Window.partitionBy("Emp Department").orderBy(col("Emp Salary").desc)
  df.withColumn("row No.", row_number.over(windowSpec2))
    .show(false)

  //rank
  df.withColumn("rank", rank.over(windowSpec))
    .show(false)

  //dense rank
  df.withColumn("dense rank", dense_rank.over(windowSpec))
    .show(false)

  df.withColumn("lag value", lag("Emp Salary", 1).over(windowSpec))
    .show(false)

  df.withColumn("lead value", lead("Emp Salary", 2).over(windowSpec))
    .show(false)

  val windowSpec3 = Window.partitionBy("Emp Department").orderBy(col("Emp Salary").desc)
  val df_stats = df
    .withColumn("row_number", row_number.over(windowSpec3))
    .withColumn("avg", avg(col("Emp Salary")).over(windowSpec3))
    .withColumn("min", min(col("Emp Salary")).over(windowSpec3))
    .withColumn("max", max(col("Emp Salary")).over(windowSpec3))
    .withColumn("sum", sum(col("Emp Salary")).over(windowSpec3))

  df_stats.select("Emp Department", "avg", "min", "max", "sum", "row_number")
    .where(col("row_number") === 1)
    .show(false)

  val topSalariedEmp = df.withColumn("rank", rank.over(windowSpec3))
    .where(col("rank") === 1)
    .show(false)

  df.withColumn("cumeDistance", cume_dist().over(windowSpec3))
    .show(false)

  df.withColumn("nthrank", nth_value(col("Emp Salary"), 2).over(windowSpec3))
    .show(false)

  df.withColumn("ntile", ntile(4).over(windowSpec3))
    .show(false)

  Thread.sleep(6000)

  spark.stop()
}

