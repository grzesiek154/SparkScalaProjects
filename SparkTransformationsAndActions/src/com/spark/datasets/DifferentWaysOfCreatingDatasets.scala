package com.spark.datasets
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DifferentWaysOfCreatingDatasets {
  
//  There are a few ways to create a Dataset, but the first thing you need to do is to define a
//domain-specific object to represent each row. The first way is to transform a DataFrame
//to a Dataset using the as(Symbol) function of the DataFrame class. The second way is
//to use the SparkSession.createDataset() function to create a Dataset from a local
//collection objects. The third way is to use the toDS implicit conversion utility. Listing 4-38
//provides examples of creating Datasets using the different ways described earlier.
  
  case class Movie(actor_name:String, movie_title:String, produced_year:Long)
      
  
   def main(args: Array[String]) {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DifferentWaysOfCreatingDatasets")
      .getOrCreate()
      
    import spark.implicits._
      
    val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
      
    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]
    // "as" is a function from DataFram java class
      
    // create a Dataset using SparkSession.createDataset() and the toDS
    val localMovies = Seq(Movie("John Doe", "Awesome Movie", 2018L), Movie("Mary Jane", "Awesome Movie", 2018L))
    
    val localMoviesDS1 = spark.createDataset(localMovies)
    val localMoviesDS2 = localMovies.toDS()
    //localMoviesDS1.show()
    
    movies.show()
    
    

//    Out of the three ways of creating Datasets, the first way is the most popular one.
//During the process of transforming a DataFrame to a Dataset using a Scala case class,
//Spark will perform a validation to ensure the member variable names in the Scala case
//class matches up with the column names in the schema of the DataFrame. If there is a
//mismatch, Spark will let you know
    
   }
  
}