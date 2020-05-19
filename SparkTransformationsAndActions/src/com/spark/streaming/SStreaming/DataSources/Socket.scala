package com.spark.streaming.SStreaming.DataSources
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


object Socket extends App {
  
    Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("StructuredStreamingIntroduction")
    .getOrCreate()

  import spark.implicits._
  
  
  val socketDF = spark.read.format("socket")
                          .option("host", "localhost")
                          .option("port", "9999").load()
  val words = socketDF.as[String].flatMap(_.split(" "))
  val wordCounts = words.groupBy("value").count()
  
  val query = wordCounts.writeStream.format("console").outputMode("complete").start()
}