package com.spark.sql.aggregations.with_grouping
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollectionGroupValues extends App{
  
//  The functions collect_list(col) and collect_set(col) are useful for collecting all the
//values of a particular group after the grouping is applied. Once the values of each group
//are collected into a collection, then there is freedom to operate on them in any way
//you choose. There is one small difference between the returned collection of these two
//functions, which is the uniqueness. The collection_list function returns a collection
//that may contain duplicate values, and the collection_set function returns a collection
//that contains only unique values. 
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val flight_summary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").
    load("../data/beginning-apache-spark-2-master/chapter5/data/flights/flight-summary.csv")
    
    val highCountDestCities = flight_summary.where('count > 5500)
      .groupBy('origin_state).agg(collect_list("dest_city").as("dest_cities"))

      
      //
      highCountDestCities.withColumn("dest_city_count", size('dest_cities)).show(5)
      highCountDestCities.withColumn("dest_city_count", size('dest_cities)).show(5, false)
      
      
}