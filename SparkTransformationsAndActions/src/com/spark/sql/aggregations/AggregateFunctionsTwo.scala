package com.spark.sql.aggregations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctionsTwo extends App{
  
     Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()
      
   import spark.implicits._
   
    val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
    load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")
    
    flight_summary.select(min("count"), max("count")).show
    
    
//    sum(col)
//This function computes the sum of the values in a numeric column. Listing 5-7 performs
//the sum of all the flights in the flight_summary dataset.
    flight_summary.select(sum("count")).show
    
//    sumDistinct(col)
//This function does what it sounds like. It sums up only the distinct values of a numeric
//column. The sum of the distinct counts in the flight_summary DataFrame should be less
//than the total sum displayed in Listing 5-7. See Listing 5-8 for computing the sum of the
//distinct values
    
    flight_summary.select(sumDistinct("count")).show
    
    
    
}