//package com.spark.streaming.SStreaming
//import org.apache.log4j._
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.streaming._
//
//
//
////Structured Streaming supports joining two streaming
////DataFrames. Given the unbounded nature of a streaming DataFrame, Structured
////Streaming must maintain the past data of both streaming DataFrames to match with
////any future, yet-to-be-received data. To avoid the explosion of the streaming state that
////Structured Streaming must maintain, a watermark can be optionally provided for both
////streaming DataFrames, and a constraint on event time must be defined in the join
////condition. Let’s go through an IoT use case of joining two data sensor–related data
////streams of a data center. The first one contains the temperature reading of the various
////locations in a data center, and the second one contains the load information of each
////computer in the same data center. The join condition of these two streams is based on
////the location. 
//object JoinOperationsWatermark {
//  
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//  val spark = SparkSession.builder
//    .master("local[*]")
//    .appName("StructuredStreamingIntroduction")
//    .getOrCreate()
//
//  import spark.implicits._
//  
//  import org.apache.spark.sql.functions.expr
//// the specific streaming data source information is not important in this
//example
//val tempDataDF = spark.readStream. ...
//val loadDataDF = spark.readStream. ...
//val tempDataWatermarkDF = tempDataDF.withWaterMark("temp_taken_time",
//"1 hour")
//val loadDataWatermarkDF = loadDataDF.withWaterMark("load_taken_time",
//"2 hours")
//// join on the location id as well as the event time constraint
//tempWithLoadDataDF = tempDataWatermarkDF.join(loadDataWatermarkDF,
//expr(""" temp_location_id = load_location_id AND
//load_taken_time >= temp_taken_time AND
//load_taken_time <= temp_taken_time + interval 1 hour
//""")
//)
//  
//}