package DataFrameAndSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object WindowFunctionsPlayground extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()

  import spark.implicits._

  val appleDataSchema = StructType(Array(
    StructField("date", StringType, true),
    StructField("open", DoubleType, true),
    StructField("high", DoubleType, true),
    StructField("low", DoubleType, true),
    StructField("close", DoubleType, true),
    StructField("adj_close", DoubleType, true),
    StructField("volume", DoubleType, true)
  ))

  val flightSummaryDF = spark.read.option("header", true).csv("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")

  val temAppleDateDF = spark.read.schema(appleDataSchema).csv("../data/beginning-apache-spark-2-master/chapter5/data/stocks/apple-three-year.csv")
  val header = temAppleDateDF.first()
  val appleStockDataDF = temAppleDateDF.filter(line => line != header)
  val appleStockDataWithMonthDF = appleStockDataDF.withColumn("month", month('Date))


  val destAirportWindow = Window.partitionBy('dest_city).orderBy('origin_city.desc)
  val destAirportWindow2 = Window.partitionBy('dest_city).orderBy('origin_city).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  val stateRankDF = flightSummaryDF.withColumn("rank", rank().over(destAirportWindow))
  val stateDenseRankDF = flightSummaryDF.withColumn("rank", dense_rank().over(destAirportWindow))
  val countAllDF = flightSummaryDF.withColumn("count all", count("*").over(destAirportWindow2))

  val appleStockDataWindow = Window.partitionBy('month).orderBy("adj_close")
  val appleStockDataWindowWithRange = Window.partitionBy('month).orderBy("adj_close").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  val avgValuePerMonth = appleStockDataWithMonthDF.withColumn("avg close", avg("adj_close")
    .over(appleStockDataWindowWithRange)).where('month === 1)

  val maxValuePerMonth = appleStockDataWithMonthDF.withColumn("max close", max("close")
    .over(appleStockDataWindowWithRange)).where('month === 1)

  //time window usage
  val appleMonthly = appleStockDataDF.groupBy(window('Date, "30 days", "30 days")).agg(avg('close).as("average"))

  val maxData = appleStockDataWithMonthDF.groupBy("month").agg(round(max('close), 2)).orderBy('month.desc)
  val avgData = appleStockDataWithMonthDF.groupBy("month").agg(round(avg('close), 2)).orderBy('month.desc)

  maxValuePerMonth.show(5)
  appleMonthly.select("window.start", "window.end", "average").orderBy("window.start").show(10)


}