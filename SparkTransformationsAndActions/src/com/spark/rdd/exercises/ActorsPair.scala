package com.spark.rdd.exercises
import org.apache.log4j._
import org.apache.spark.SparkContext


//Determine which pair of actors worked together most. Working together
//is defined as appearing in the same movie. the output should have three
//columns: actor 1, actor 2, and count. the output should be sorted by the count
//in descending order. the solution to this question will require a self-join.
object ActorsPair {
  
  def countAppuerance () ={
    
    
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","ActorsPair")
    
     val data = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")
     val data2 = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")   
     val ratingTitleYear = data.map(line => (line.split("\t")(0).toDouble, line.split("\t")(1), line.split("\t")(2).toInt))
     val moviesAndActors = data2.map(line => (line.split("\t")(1),( line.split("\t")(0), line.split("\t")(2).toInt)))
     
    
      val collectedActors = moviesAndActors.reduceByKey{
       case ((actor, year), (actor2, year2)) => ((actor + actor2) , year)
       
     }
        
     val titleAndActor = collectedActors.mapValues{
       case (actor,year) => (actor.trim())
              
     }    
     val asd = titleAndActor.map{
       case(title, actors) => ((title, actors)._1, (title, actors)._2)
     }
     asd.sortByKey().foreach(println)
    
//     to consider:
//     flatMap - returns sequence of data
  }
}