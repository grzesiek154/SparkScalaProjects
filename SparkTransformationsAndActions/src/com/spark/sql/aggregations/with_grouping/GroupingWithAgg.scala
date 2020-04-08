package com.spark.sql.aggregations.with_grouping
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupingWithAgg extends App{
  
  
//  This section covers aggregation with grouping of one or more columns. The aggregations
//are usually performed on datasets that contain one or more categorical columns, which
//have low cardinality. Examples of categorical values are gender, age, city name, or
//country name. The aggregations will be done through the functions that are similar to
//the ones mentioned earlier. However, instead of performing aggregation on the global
//group in a DataFrame, they will perform the aggregation on each of the subgroups inside
//a DataFrame.
  
//  Performing aggregation with grouping is a two-step process. The first step is to
//perform the grouping by using the groupBy(col1,col2,...) transformation, and that’s
//where you specify which columns to group the rows by. Unlike other transformations
//that return a DataFrame, the groupBy transformation returns an instance of the
//RelationalGroupedDataset class, which you then can apply one or more aggregation
//functions to. Listing 5-12 demonstrates a simple grouping of using one column and
//one aggregation. Notice that the groupBy columns will automatically be included in the
//output.
    
   Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()
      
   import spark.implicits._
   
    val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
    load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")
    
    flight_summary.show(5)
    flight_summary.groupBy("origin_airport").count().show(5, false)
    flight_summary.groupBy('origin_state, 'origin_city).count().where('origin_state === "CA").orderBy('coun.desc).show(5)
    
//    In addition to grouping by two columns, the previous statement filters the rows
//to only the ones with a CA state. The orderBy transformation is used to make it easier
//to identify which city has the most number of options in terms of destination airport.
//It makes sense that both San Francisco and Los Angeles in California have the largest
//number of destination airports that one can fly to.
//The class RelationalGroupedDataset provides a standard set of aggregation
//functions that you can apply to each subgroup. They are avg(cols), count(),
//mean(cols), min(cols), max(cols), and sum(cols). Except for the count() function, all
//the remaining ones operate on numeric columns.
    
    
}