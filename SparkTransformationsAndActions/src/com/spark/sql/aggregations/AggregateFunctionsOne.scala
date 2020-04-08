package com.spark.sql.aggregations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctionsOne extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  //   count(col)
  //Counting is a commonly used aggregation to find out the number of items in a group.
  //Listing 5-2 computes the count for both the origin_airport and dest_airport
  //columns, and as expected, the count is the same. To improve the readability of the result
  //column, you can use the as function to give it a friendlier column name. Notice that you
  //need to call the show action to see the result.

  val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
    load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")

  flight_summary.select(count("origin_airport"), count("dest_airport").as("dest_count")).show

  case class Movie(actor_name: String, movie_title: String, produced_year: Long)
  val badMoviesDF = Seq(
    Movie(null, null, 2018L),
    Movie("John Doe", "Awesome Movie", 2018L),
    Movie(null, "Awesome Movie", 2018L),
    Movie(null, "Batman", 2006L),
    Movie("Mary Jane", "Awesome Movie", 2018L)).toDF
  badMoviesDF.show

  // now performing the count aggregation on different columns
  badMoviesDF.select(count("actor_name"), count("movie_title"),
    count("produced_year"), count("*")).show
    
    
    
//    countDistinct(col)
//This function does what it sounds like. It counts only the unique items per group. The
//output in Listing 5-4 shows the difference in the count result between the countDistinct
//function and the count function. As it turns out, there are 322 unique airports in the
//flight_summary dataset.
    
    
    flight_summary.select(countDistinct("origin_airport"), countDistinct("dest_airport"), count("*")).show
    
    
//    approx_count_distinct (col, max_estimated_error=0.05)
//Counting the exact number of unique items in each group in a large dataset is an
//expensive and time-consuming operation. In some use cases, it is sufficient to have
//an approximate unique count. One of those use cases is in the online advertising
//business where there are hundreds of millions of ad impressions per hour and there is
//a need to generate a report to show the number of unique visitors per certain type of
//member segment. Approximating a count of distinct items is a well-known problem in
//the computer science field, and it is also known as the cardinality estimation problem.
//Luckily, there is already a well-known algorithm called HyperLogLog (https://
//en.wikipedia.org/wiki/HyperLogLog) that you can use to solve this problem, and
//Spark has implemented a version of this algorithm inside the approx_count_distinct
//function. Since the unique count is an approximation, there will be a certain amount of
//error. This function allows you to specify a value for an acceptable estimation error for
//this use case. Listing 5-5 demonstrates the usage and behavior of the approx_count_
//distinct function. As you dial down the estimation error, it will take longer and longer
//for this function to complete and return the result
    
    flight_summary.select(count("count"), countDistinct("count"), approx_count_distinct("count", 0.05)).show()
}