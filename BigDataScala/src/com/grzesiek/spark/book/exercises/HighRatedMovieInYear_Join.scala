package com.grzesiek.spark.book.exercises

import org.apache.log4j._
import org.apache.spark.SparkContext

//Compute the highest-rated movie per year and include all the actors in that
//movie. the output should have only one movie per year, and it should contain
//four columns: year, movie title, rating, and a semicolon-separated list of
//actor names. this question will require joining the movies.tsv and movieratings.tsv files. there are two approaches to this problem. the first one
//is to figure out the highest-rated movie per year first and then join with the list
//of actors. the second one is to perform the join first and then figure out the
//highest-rated movies per year along with a list of actors. the result of each
//approach is different than the other one. Why do you think that is?

object HighRatedMovieInYear_Join {
  
//  
//  def getMaxRating(lines: (String,String,Int)) = {
//    
//      lines.map(x => (if (x._3 > y._3) x._3 else y._3 ,)
//          
//          for line <- lines
//  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
     val sc = new SparkContext("local[*]","HighRatedMovieInYear_Join")
    
     val data = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")      
     val ratingTitleYear = data.map(line => (line.split("\t")(0).toDouble, line.split("\t")(1), line.split("\t")(2).toInt))
     
     val dataReorder = ratingTitleYear.map(x => ( x._3, (x._2, x._1))).sortByKey(false).collect()
     //val highestRatedMovie = dataReorder.sortByKey()
     dataReorder.foreach(println)
     


     
    
  }
}