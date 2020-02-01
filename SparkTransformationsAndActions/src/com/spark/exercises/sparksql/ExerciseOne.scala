package com.spark.exercises.sparksql
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object ExerciseOne {
  
  case class Movie(actor_name:String, movie_title:String, produced_year:Long)
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
      val moviesDS = movies.as[Movie]
    
      val moviesInEachYear = moviesDS.
      
  }
}