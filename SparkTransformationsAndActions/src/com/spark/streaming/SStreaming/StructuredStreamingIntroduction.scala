package com.spark.streaming.SStreaming
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object StructuredStreamingIntroduction extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("StructuredStreamingIntroduction")
    .getOrCreate()

  import spark.implicits._

  val mobileDataSchema = new StructType().add("id", StringType, false)
    .add("action", StringType, false)
    .add("ts", TimestampType, false)

  val mobileDataDF = spark.read.json("../data/beginning-apache-spark-2-master/chapter6/data/mobile/file1.json")
  //  By default, Structured Streaming requires a schema when reading data from a filebased data source. This makes sense because initially the directory might be empty, so
  //therefore Structured Streaming wouldn’t be able to infer the schema. However, if you
  //really want it to infer the schema, you can set the configuration spark.sql.streaming.
  //schemaInference to true. In this example, you will explicitly create a schema

  val mobileSSDF = spark.readStream.schema(mobileDataSchema).json("../data/beginning-apache-spark-2-master/chapter6/data/mobile/input")

  mobileSSDF.isStreaming

  val actionCountDF = mobileSSDF.groupBy(window($"ts", "10 minutes"), $"action").count
  //  def window(timeColumn: Column, windowDuration: String): Column
  //  Generates tumbling time windows given a timestamp specifying column.

  val mobileConsoleSQ = actionCountDF.writeStream.format("console").option("truncate", "false")
    .outputMode("complete").start()

  //mobileConsoleSQ.awaitTermination()

  println(mobileConsoleSQ.status)
  //  The status() function will tell you what’s going at the current status of the query stream, which can
  //be either in wait mode or in the middle of processing the current batch of events
  println(mobileConsoleSQ.lastProgress)
  //  The lastProgress() function provides some metrics about the processing of the last batch
  //  of events including processing rates, latencies, and so on

  mobileConsoleSQ.awaitTermination()
  //  StreamingQuery.awaitTermination() function, which is a blocking call to prevent the
  //driver process from exiting, and to let the streaming query continuously run and process
  //new data as it arrives into the data source.

  // stop a streaming query
  mobileConsoleSQ.stop
  // another way of stopping all streaming queries in a Spark application
  for (qs <- spark.streams.active) {
    println(s"Stop streaming query: ${qs.name} - active: ${qs.isActive}")
    if (qs.isActive) {
      qs.stop
    }
  }

}