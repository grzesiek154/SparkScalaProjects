package com.spark.sql.adv_analytics_functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Time window aggregations can help with answering questions such as what is the weekly average
//closing price of Apple stock or the monthly moving average closing price of Apple stock
//across each week.
//Window functions come in a few versions, but they all require a timestamp type
//column and a window length, which can be specified in seconds, minutes, hours, days,
//or weeks. The window length represents a time window that has a start time and end
//time, and it is used to determine which bucket a particular piece of time-series data
//should belong to. Another version takes additional input for the sliding window size,
//which tells how much a time window should slide by when calculating the next bucket

object TimeWindowFunction extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val apple_data = spark.read.option("header", true).option("inferSchema", true)
    .csv("../data/beginning-apache-spark-2-master/chapter5/data/stocks/aapl-2017.csv")

  //apple_data.show(5)

  // calculate the weekly average price using window function inside the groupBy transformation
  // this is an example of the tumbling window, aka fixed window

  val appleWeeklyAvgDF = apple_data.groupBy(window('Date, "1 week"))
    .agg(avg("Close").as("weekly_avg"))

  appleWeeklyAvgDF.orderBy("window.start")
    .selectExpr("window.start", "window.end", "round(weekly_avg, 2) as weekly_avg").show(5)

  // 4 weeks window length and slide by one week each time
  val appleMonthlyAvgDF = apple_data.groupBy(window('Date, "4 week", "1 week")).
    agg(avg("Close").as("monthly_avg"))

  // display the results with order by start time
  appleMonthlyAvgDF.orderBy("window.start")
    .selectExpr("window.start", "window.end",
      "round(monthly_avg, 2) as monthly_avg")
    .show(5)
}