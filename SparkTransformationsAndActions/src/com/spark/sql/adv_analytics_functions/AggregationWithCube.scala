package com.spark.sql.adv_analytics_functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationWithCube extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  //  A cube is basically a more advanced version of a rollup. It performs the aggregations
  //across all the combinations of the grouping columns. Therefore, the result includes what
  //a rollup provides as well as other combinations. In our example of cubing by the origin_
  //state and origin_city, the result will include the aggregation for each of the original
  //cities

  //  The cube function “takes a list of columns and applies aggregate expressions
  //  to all possible combinations of the grouping columns”.

  val filight_summary = spark.read.option("header", true).option("inferSchema", true)
    .csv("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")

  val twoStatesSummary = filight_summary.select('origin_city, 'origin_state, 'count).
    where('origin_state === "CA"
      || 'origin_state === "NY")
    .where('count > 1 && 'count < 20)
    .where('origin_city =!= "WhitePlains")
    .where('origin_city =!= "Newburgh")
    .where('origin_city =!= "Mammoth Lakes")
    .where('origin_city =!= "Ontario")

  twoStatesSummary.cube('origin_state, 'origin_city).
    agg(sum("count") as "total count")
    .orderBy('origin_state.asc_nulls_last, 'origin_city.asc_nulls_last).show
    
      twoStatesSummary.cube('origin_city, 'origin_state).
    agg(sum("count") as "total count")
    .orderBy('origin_state.asc_nulls_last, 'origin_city.asc_nulls_last).show
}