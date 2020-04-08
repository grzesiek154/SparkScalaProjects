package com.spark.rdd.exercises
import org.apache.log4j._
import org.apache.spark.SparkContext


//Determine which pair of actors worked together most. Working together
//is defined as appearing in the same movie. the output should have three
//columns: actor 1, actor 2, and count. the output should be sorted by the count
//in descending order. the solution to this question will require a self-join.


// czy self join jest po to aby z jednego rdd policzyc count dla par a z drugieo wyswietlic nazwiska actorow

case class Exercise4 (actor1: String, actor2:String, count:Int)
case class ActorsPair (actor1: String, actor2: String)
case class MovieAndActor(actor:String, movieTile: String)
case class MovieAndActors(movieTile: String, actors:Seq[String])
case class ActorAndMovies(actor: String, movies:Seq[String])

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
     val actorAndMovies = data2.map(line => (line.split("\t")(0),( line.split("\t")(1), line.split("\t")(2).toInt)))
     
    
      val collectedActors = moviesAndActors.reduceByKey{
       case ((actor, year), (actor2, year2)) => ((actor + actor2) , year)
       
    }
    
     val collectedMovies = actorAndMovies.reduceByKey{
       case ((movie, year), (movie2, year2)) => ((movie + movie2) , year)
       
    }
     
     // zawartisc eg. (ActorAndMovies(Fey, Tina,List(MegamindMean GirlsDate NightBaby MamaGake no ue no Ponyo))
     val collectedMoviesMap = collectedMovies.flatMap{ line =>
       Seq(ActorAndMovies(line._1,Seq(line._2._1)))
     }
    
    //val test2 = moviesAndActors.flatMap(line => Seq( )
    
    val selfJoin = actorAndMovies.join(actorAndMovies)
        
     val titleAndActor = moviesAndActors.flatMap{ line =>
       val actor = line._1
       val title = line._2._1
       Seq(MovieAndActor(actor, title))
     }
     
     
             
       val test = titleAndActor.flatMap( line => if (line.movieTile == line.movieTile) Seq(ActorsPair(line.actor, line.actor)) else Seq.empty )
       
       // mozna zrobic self join dla zredukowanego tuple(actor , movies) oraz (actor, movie)
       //val test2 = collectedMoviesMap.map((actor, movies) => if (actor != actor) && (movies.contains(movie))
       
       
       

  

  }
}