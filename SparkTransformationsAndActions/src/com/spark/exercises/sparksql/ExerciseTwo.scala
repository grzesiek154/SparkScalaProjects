package com.spark.exercises.sparksql

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.Optional
//Compute the number of movies each actor was in. the output should have
//two columns: actor and count. the output should be ordered by the count in
//descending order.
object ExerciseTwo {
  
   case class Movie(actor_name:String, movie_title:String, produced_year:Option[Long])
   case class YearAndMoviesAmount(year:String, number_of_movies:Long)

  
   def main(args: Array[String]) {
     
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
   
      
      val moviesDS = movies.as[Movie]
      
      val actorsMovies = moviesDS.groupBy('actor_name).count.orderBy('count.desc)
      
      actorsMovies.show()
      
    }
  
}