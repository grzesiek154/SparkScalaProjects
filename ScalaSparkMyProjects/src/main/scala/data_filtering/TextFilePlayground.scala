package data_filtering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


import scala.io.Source

object TextFilePlayground extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()
  import spark.implicits._

  val fileName = "book.txt"

  def readFile(fileName: String): List[String] = {
    val file = Source.fromFile(fileName, "ISO-8859-1")
    val fileLines = file.getLines()
    fileLines.toList
  }

  val textFileDF = spark.sparkContext.makeRDD(readFile(fileName)).toDF()
  val textFileRDD = spark.sparkContext.makeRDD(readFile(fileName))



  def countWordOccurrence(searchedWord: String, rdd: RDD[String]) = {


    val wordFound = rdd.flatMap(line => line.split("\\W+").filter(word => word.contains(searchedWord)))
    wordFound.countByValue().foreach(println)
  }

  def findWord(word: Option[String]) = {
    word match {
      case Some(w) => true
      case  None => false
    }
  }

  def countExactWordOccurrence(searchedWord: String, rdd: RDD[String]) = {
    val wordPattern = s"^$searchedWord".r
    val wordFound = rdd.flatMap(line => line.split("\\W+")).filter(word => word == searchedWord)
    val result = wordFound.map(word => word.toLowerCase()).countByValue()
    result.foreach(println)
  }

  def countAllWordsOccurrence(rdd: RDD[String]) = {
    val wordOccurrence =  rdd.flatMap(line => line.split("\\W+")).filter(word =>  word.trim.replace(";","").length > 0).
                                                            map(word => (word, 1)).reduceByKey((a,b) => a+b).sortBy(_._2,false)
    wordOccurrence.collect().foreach(println)
  }

  def countAllWordsOccurrence2(rdd: RDD[String]) = {
    val words =  rdd.flatMap(line => line.split("\\W+"))
    val lowerCaseWords = words.map(word => word.toLowerCase())
    lowerCaseWords.countByValue().foreach(println)
  }
   countExactWordOccurrence("rate", textFileRDD)



}
