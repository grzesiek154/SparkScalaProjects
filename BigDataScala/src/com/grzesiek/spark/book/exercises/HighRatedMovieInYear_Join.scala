package com.grzesiek.spark.book.exercises

import org.apache.log4j._
import org.apache.spark.SparkContext

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
     val ratingTitleYear = data.map(line => (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2).toInt))
     
     val dataReorder = ratingTitleYear.map(x =>( x._3, (x._2, x._1)))
     //val ratingTitleYearFiltred = dataReorder.filter()
     
     //ratingTitleYearFiltred.foreach(println)
  }
}