package data_filtering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WaysOfRemovingHeaders extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()
  val customerOrders= spark.sparkContext.textFile("../data/vix-daily.csv").filter(!_.contains("Date"))

  val customerOrdersDF = spark.read.csv("../data/vix-daily.csv")
  val headers = customerOrdersDF.first()
  val customerOrdersDF2 = customerOrdersDF.filter(line => line != headers)

  customerOrders.foreach(println)

}
