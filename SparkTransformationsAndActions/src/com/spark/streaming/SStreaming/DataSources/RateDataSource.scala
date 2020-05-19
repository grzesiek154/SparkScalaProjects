package com.spark.streaming.SStreaming.DataSources
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object RateDataSource extends App{
  
  
      Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("StructuredStreamingIntroduction")
    .getOrCreate()

  import spark.implicits._
  
  // configure it to generate 10 rows per second
val rateSourceDF = spark.readStream.format("rate")
.option("rowsPerSecond","10")
.load()

val rateQuery = rateSourceDF.writeStream
.outputMode("update")
.format("console")
.option("truncate", "false")
.start()

rateQuery.awaitTermination()
  
}