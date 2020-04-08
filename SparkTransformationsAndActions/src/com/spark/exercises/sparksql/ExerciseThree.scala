package com.spark.exercises.sparksql

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions.max

//Compute the highest-rated movie per year and include all the actors in that
//movie. the output should have only one movie per year, and it should contain
//four columns: year, movie title, rating, and a semicolon-separated list of
//actor names. this question will require joining the movies.tsv and movieratings.tsv files. there are two approaches to this problem. the first one
//is to figure out the highest-rated movie per year first and then join with the list
//of actors. the second one is to perform the join first and then figure out the
//highest-rated movies per year along with a list of actors. the result of each
//approach is different than the other one. Why do you think that is?
object ExerciseThree {
  
  case class MovieRatings (rating:String, movie_title:String, produced_year:String)
  case class MovieAndActors (actor:String, title:String, year:String)
  
   def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
       val movieRatingsSchema = StructType(Array(StructField("rating", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", StringType, true)))
        val movesAndActorsSchema = StructType(Array(StructField("actor", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", StringType, true)))
      
      val movies = spark.read.option("delimiter", "\t").schema(movesAndActorsSchema).csv("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")
      val moviesRatings = spark.read.option("delimiter", "\t").schema(movieRatingsSchema).csv("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")
      
    

      //1. Get the highest rated movie per year and join actors
      // how to pass a function to agg wich will collect all actors for particlar movie
      val movieWithActors = movies.select("movie_title", "actor").groupBy("movie_title")
      
      
      
      
      moviesRatings.show()
      movies.show()
 
      
   }
  
}