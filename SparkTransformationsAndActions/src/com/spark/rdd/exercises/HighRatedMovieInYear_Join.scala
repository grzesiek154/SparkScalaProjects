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
  

     def createActorsTuple (actor: String) ={
       
       val actorsList = List.apply(actor)
     }

  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
     val sc = new SparkContext("local[*]","HighRatedMovieInYear_Join")
    
     val data = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")
     val data2 = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")   
     val ratingTitleYear = data.map(line => (line.split("\t")(0).toDouble, line.split("\t")(1), line.split("\t")(2).toInt))
     val moviesAndActors = data2.map(line => (line.split("\t")(1),( line.split("\t")(0), line.split("\t")(2).toInt)))
     
     // 1 Approach: figure out the highest-rated movie per year
     val dataReorder = ratingTitleYear.map(x => ( x._3, (x._2, x._1)))
     // solution description: tuple(val, tuple2), we are checking tuple2._2 value in tupel2 and comparing each other,
     // than we return tuple2 with the highest value on tuple2._2
     val solutionOne = dataReorder.reduceByKey((total,value) => if (total._2 < value._2) value else total).sortByKey()
     
     //shifting data befor join, movie title need to be a key
     val highestRatedMoviesPerYear = solutionOne.map(value => (value._2._1, (value._1, value._2._2)))
   
     
     val orderedData = dataReorder.sortByKey().collect();
   
    // (movieTitle, (actor, movieYear))
    // collecting actors from the same movie
     val collectedActors = moviesAndActors.reduceByKey{
       case ((actor, year), (actor2, year2)) => ((actor + actor2) , year)
     }      
     val joinedRDDSolutionOne = highestRatedMoviesPerYear.join(collectedActors)
     //joinedRDDSolutionOne.take(20).foreach(println)
        
 
  
     //2 APPROACH: 


    val ratingTitleYear2 = ratingTitleYear.map(value => (value._2, (value._3, value._1)))
    val moviesAndActors2 = moviesAndActors.map(value => (value._1, (value._2._2, value._2._1)))
    //(value => (value._1,(value._2._1._1, value._2._1._2, value._2._2._2)))
    val ratingAndActors = ratingTitleYear2.join(moviesAndActors2).reduceByKey{
       case (((yearTotal, ratingTotal), (year, actorTotal)), ((year2, rating), (year3, actor))) => if (ratingTotal < rating) ((yearTotal, rating),(yearTotal, actor)) 
       else ((yearTotal, ratingTotal),(yearTotal, actor))
     }
    
    ratingAndActors.sortByKey().foreach(println)
     //ratingTitleYear2.foreach(println)
    
  }
}