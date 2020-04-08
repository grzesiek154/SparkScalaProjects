package com.spark.sql.aggregations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Introduction extends App{
  
//  Performing any interesting analytics on big data usually involves some kind of
//aggregation to summarize the data in order to extract patterns or insights or to generate
//summary reports. Aggregations usually require some form of grouping either on the
//entire dataset or on one or more columns, and then they apply aggregation functions
//such as summing, counting, or averaging to each group. Spark provides many commonly
//used aggregation functions as well as the ability to aggregate the values into a collection,
//which then can be further analyzed. The grouping of rows can be done at different levels,
//and Spark supports the following levels:
//• Treat a DataFrame as one group.
//• Divide a DataFrame into multiple groups by using one or more columns
//and perform one or more aggregations on each of those groups.
//• Divide a DataFrame into multiple windows and perform moving
//average, cumulative sum, or ranking. If a window is based on time,
//the aggregations can be done with tumbling or sliding windows.
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()
  
 val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
                                 load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")
                                 
 flight_summary.count()                      
}