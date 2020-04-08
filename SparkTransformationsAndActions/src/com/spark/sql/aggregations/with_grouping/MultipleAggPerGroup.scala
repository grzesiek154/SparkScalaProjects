package com.spark.sql.aggregations.with_grouping
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultipleAggPerGroup extends App {

  //  Sometimes there is a need to perform multiple aggregations per group at the same time.
  //For example, in addition to the count, you also would like to know the minimum and
  //maximum values. The RelationalGroupedDataset class provides a powerful function
  //called agg that takes one or more column expressions, which means you can use any of
  //the aggregation functions including the ones listed in Table 5-1. One cool thing is these
  //aggregation functions return an instance of the Column class so you can then apply any
  //of the column expressions using the provided functions. A common need is to rename
  //the column after the aggregation is done to make it shorter, more readable, and easier to
  //refer to.

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
    load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")
    
  flight_summary.show(5)

  flight_summary.groupBy('origin_airport).agg(
    count("count").as("count"),
    min("count").as("min values"),
    max("count"),
    sum("count")).show(5)

//    By default the aggregation column name is the aggregation expression, which makes
//the column name a bit long and not easy to refer to. A common pattern is to use the
//Column.as function to rename the column to something more suitable.
//The versatile agg function provides an additional way to express the column
//expressions via a string-based key-value map. The key is the column name, and the value
//is an aggregation function, which can be avg, max, min, sum, or count.
  flight_summary.groupBy("origin_airport")
    .agg(
      "count" -> "count",
      "count" -> "min",
      "count" -> "max",
      "count" -> "sum")
    .show(5)
    
//    The result is the same as in Listing 5-14. Notice there isn’t an easy way to rename the
//aggregation result column name. One advantage this approach has over the first one is the
//map can programmatically be generated. When writing production ETL jobs or performing
//exploratory analysis, the first approach is used more often than the second one.

}