package com.udemy
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import scala.math.sqrt

object MovieSimilarities {

  def loadMovies(): Map[Int, String] = {

    //handling character encoding issue
    implicit val codec = Codec("UTF-8")

    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create map from ints to string and populati it with values from u.items
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("../data/ml-100k/u.item").getLines()
    //    for (line <- lines) {
    //      val fields = line.split('|')
    //      if (fields.length > 1) {
    //        movieNames += (fields(0).toInt -> fields(1))
    //      }
    //    }
    lines.map(line => line.split('|')).filter(fileds => fileds.length > 1)
      .foreach(fields => movieNames += (fields(0).toInt -> fields(1)))

    return movieNames
  }

  case class MovieRatingCC(movieId: Int, movieRating: Double)
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {

    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movieId1 = movieRating1._1
    val movieId2 = movieRating2._1

    // if we have a two combinations of movie pairs AB and BA so in this case only if A is lees than B
    // only first combinations will survive
    return movieId1 < movieId2
  }

  // we create a tuple with ((movie1, movie2 and corresponding rationg (rating1, rating2)) in order to compute 
  // smilarities base on tupe values
  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.FATAl) 
    

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.textFile("../data/ml-100k/u.data")
    val nameDict = loadMovies()

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)

    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()
    
    
    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    
    //safe results if desired
        //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")
    
    // Extract similarities for the movie we care about that are "good".
    
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0 //movies that where watched by at leats 50 people
      
      // this is id of movie which we will pass as comand line argument
      val movieID:Int = args(0).toInt
      
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     
      
      val filteredResults = moviePairSimilarities.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }



  }
}