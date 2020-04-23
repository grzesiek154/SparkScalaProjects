package com.spark.sql.adv_analytics_functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//   Aggregation with Rollups and Cubes
//Rollups and cube are basically more advanced versions of grouping on multiple
//columns, and they are generally used to generate subtotals and grand totals across the
//combinations and permutations of those columns. The order of the provided set of
//columns is treated as a hierarchy for grouping

object AggregationWithRollups extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  //Rollups
  //When working with hierarchical data such as the revenue data that spans different
  //departments and divisions, rollups can easily calculate the subtotals and a grand total
  //across them. Rollups respect the given hierarchy of the given set of rollup columns and
  //always start the rolling up process with the first column in the hierarchy. The grand total
  //is listed in the output where all the column values are null.
  
      //rollup is a subset of cube that “computes hierarchical subtotals from left to right”.
   // rollup($"word", $"num") doesn’t return the counts when only word is null.

  val filight_summary = spark.read.option("header", true).option("inferSchema", true)
    .csv("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")

  // filter data down to smaller size to make it easier to see the rollups result
  val twoStatesSummary = filight_summary.select('origin_city, 'origin_state, 'count).
    where('origin_state === "CA"
      || 'origin_state === "NY")
    .where('count > 1 && 'count < 20)
    .where('origin_city =!= "WhitePlains")
    .where('origin_city =!= "Newburgh")
    .where('origin_city =!= "Mammoth Lakes")
    .where('origin_city =!= "Ontario")

  twoStatesSummary.rollup('origin_state, 'origin_city)
    .agg(sum("count") as "total")
    .orderBy(
      'origin_state.asc_nulls_last,
      'origin_city.asc_nulls_last).show

  twoStatesSummary.groupBy('origin_state, 'origin_city)
    .agg(sum("count") as "total")
    .orderBy(
      'origin_state.asc_nulls_last,
      'origin_city.asc_nulls_last).show

  val df = Seq(
    ("bar", 2L),
    ("bar", 2L),
    ("foo", 1L),
    ("foo", 2L)).toDF("word", "num")

    
    df.rollup('word, 'num).count().sort(asc("word"), asc("num")).show
    df.cube('word, 'num).count().sort(asc("word"), asc("num")).show
    df.groupBy('word, 'num).count().sort(asc("word"), asc("num")).show

}