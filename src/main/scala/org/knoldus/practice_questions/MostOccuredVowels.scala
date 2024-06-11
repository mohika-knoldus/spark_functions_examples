package org.knoldus.practice_questions

import org.apache.spark.sql.SparkSession

object MostOccuredVowels extends App {
  val spark = SparkSession.builder()
    .appName("Most Frequent Vowel")
    .master("local[1]")
    .getOrCreate()

  val textRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/spark-hello-world-example/src/main/scala/resources/sample.txt/")
  val characterRdd = textRdd.flatMap(word => word.map(character => character))
  //val vowelsRdd = characterRdd.filter(alphabet => alphabet == 'a' || alphabet == 'e' || alphabet == 'i' || alphabet == 'o' || alphabet == 'u')
  val vowelsRdd = characterRdd.filter(character => character.toString.matches("[aeiouAEIOU]"))
  val mappedVowel = vowelsRdd.map(vowel => (vowel, 1))
  val vowelsCount = mappedVowel.reduceByKey(_ + _)
  val sortedVowelRdd = vowelsCount.sortByKey(ascending = false)
  println(sortedVowelRdd.take(1).mkString)

}
