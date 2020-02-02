package com.spark.exercises.sparksql
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.Optional


//Compute the number of movies produced in each year. the output should have
//two columns: year and count. the output should be ordered by the count in
//descending order.

object ExerciseOne {
  
  case class Movie(actor_name:String, movie_title:String, produced_year:Option[Long])
  case class MovieAndYear(movie_title:String, produced_year:Option[Long])
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
      val moviesDS = movies.as[Movie]
    
      val moviesAndYear = moviesDS.map(movie => (movie.movie_title, movie.produced_year))
      
      val moviesIn2005 = moviesAndYear.filter('_2 === 2005)
      
      //val moviesAndYearDS = moviesAndYear.as[MovieAndYear]
      
     
      
     val yearCount = moviesAndYear.groupBy('_2).count().collect()// prawdopodobnie oblicza poprawnie ilosc filmow w danym roku
                                                                 // zwrocona wartosc to row , trzeba to jakos polaczyc z konretym rokiem i stworzyc kolekcje dwoch elementow

      val yearCount2 = yearCount.mkString(",")
      
      yearCount2.foreach(println)
  }
}