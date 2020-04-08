package Exercises


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.log4j._

//Compute the highest-rated movie per year and include all the actors in that
//movie. the output should have only one movie per year, and it should contain
//four columns: year, movie title, rating, and a semicolon-separated list of
//actor names. this question will require joining the movies.tsv and movieratings.tsv files. there are two approaches to this problem. the first one
//is to figure out the highest-rated movie per year first and then join with the list
//of actors. the second one is to perform the join first and then figure out the
//highest-rated movies per year along with a list of actors. the result of each
//approach is different than the other one. Why do you think that is?

case class MoviesWithActors(title: String, actor: String, produced_year: Int)

case class MoviesWithRatings(movieTitle: String, rating: Double, produced_year: Int)

object Exercise3 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()

  import spark.implicits._

  val movieRatingsSchema = StructType(Array(StructField("rating", DoubleType, true),
    StructField("movie_title", StringType, true),
    StructField("produced_year", StringType, true)))
  val movesAndActorsSchema = StructType(Array(StructField("actor", StringType, true),
    StructField("title", StringType, true),
    StructField("produced_year", StringType, true)))

  // data for sql and data frame solution
  val moviesWithActors = spark.read.option("delimiter", "\t").schema(movesAndActorsSchema).csv("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")
  val moviesWithRatings = spark.read.option("delimiter", "\t").schema(movieRatingsSchema).csv("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")

  // data from RDD mapped to proper case class and converted to Data Frame
  val moviesWithActors2 = spark.sparkContext.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv").
    map(line => {
      val dataArr = line.split("\t")
      MoviesWithActors(dataArr(1), dataArr(0), dataArr(2).toInt)
    }).toDF()
  val moviesWithRatings2 = spark.sparkContext.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv").map(line => {
    val dataArr = line.split("\t")
    MoviesWithRatings(dataArr(1), dataArr(0).toDouble, dataArr(2).toInt)
  }).toDF()


  // getting highest rated movie per year
  val highestRatedMovies = moviesWithRatings.select('movie_title,'rating, 'produced_year).groupBy("movie_title","produced_year").max("rating").orderBy($"produced_year".desc)
  moviesWithRatings.createOrReplaceTempView("moviesRatings")
  val highestRatedMoviesSQL = spark.sql("SELECT produced_year, MAX(rating) as rating FROM moviesRatings GROUP BY produced_year ORDER BY produced_year DESC")

  // joining data converted and reordered  from RDD
  val joinedData2 = moviesWithActors2.join(moviesWithRatings2, 'title === 'movieTitle)
  // joining takane direct from rsv file to Data Frame
  val joinedData = moviesWithActors.join(highestRatedMovies).where('title === 'movie_title)

  //val highestRatedMoviesWitActors = highestRatedMovies.join()


  highestRatedMovies.show()
  joinedData.show()


  //toDo:
  //2. Join two DF (normal join and other join)
  //3. Solve the exercise

}
