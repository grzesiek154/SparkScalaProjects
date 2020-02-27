package com.spark.rdd.exercises

import org.apache.log4j._
import org.apache.spark.SparkContext
import shapeless._0

//Compute the highest-rated movie per year and include all the actors in that
//movie. the output should have only one movie per year, and it should contain
//four columns: year, movie title, rating, and a semicolon-separated list of
//actor names. this question will require joining the movies.tsv and movieratings.tsv files. there are two approaches to this problem. the first one
//is to figure out the highest-rated movie per year first and then join with the list
//of actors. the second one is to perform the join first and then figure out the
//highest-rated movies per year along with a list of actors. the result of each
//approach is different than the other one. Why do you think that is?

object HighRatedMovieInYear_Join {
  


  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
     val sc = new SparkContext("local[*]","HighRatedMovieInYear_Join")
    
     val data = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")
     val data2 = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")   
     val ratingTitleYear = data.map(line => (line.split("\t")(0).toDouble, line.split("\t")(1), line.split("\t")(2).toInt))
     val moviesAndActors = data2.map(line => (line.split("\t")(1), line.split("\t")(0), line.split("\t")(2)))
     
     // 1 Approach: figure out the highest-rated movie per year
     val dataReorder = ratingTitleYear.map(x => ( x._3, (x._2, x._1)))
     // solution description: tuple(val, tuple2), we are checking tuple2._2 value in tupel2 and comparing each other,
     // than we return tuple2 with the highest value on tuple2._2
     val solutionOne = dataReorder.reduceByKey((total,value) => if (total._2 < value._2) value else total).sortByKey().collect()
   
     
     val orderedData = dataReorder.sortByKey().collect();
    
     //val rddWihHighestRatedMovie = orderedData.map{case (,)}
    
    
     //orderedData.foreach(println)
   
     //solutionOne.foreach(println)
    
      moviesAndActors.foreach(println)
  
     


     
    
  }
}