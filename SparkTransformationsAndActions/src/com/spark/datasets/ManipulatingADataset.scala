package com.spark.datasets
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object ManipulatingADataset {

  //  Now that you have a Dataset, you can manipulate it using the transformations and
  //actions described earlier. Previously you referred to the columns in the DataFrame
  //using one of the options described earlier. With a Dataset, each row is represented by
  //a strongly typed object; therefore, you can just refer to the columns using the member
  //variable names, which will give you type safety as well as compile-time validation.
  //If there is a misspelling in the name, the compiler will flag them immediately during the
  //development phase.

  case class Movie(actor_name: String, movie_title: String, produced_year: Long)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]
    // "as" is a function from DataFram java class

    moviesDS.filter(movie => movie.produced_year == 2010).show(5)
    val firstMovie = moviesDS.first().movie_title
    println(firstMovie)

    val titleYearDS = moviesDS.map(m => (m.movie_title, m.produced_year))

    titleYearDS.printSchema()

    // demonstrating a type-safe transformation that fails at compile time, performing subtraction on a column with string type
    // a problem is not detected for DataFrame until runtime
    movies.select('movie_title - 'movie_title)

    // a problem is detected at compile time
    //moviesDS.map(m => m.movie_title - m.movie_title)

    // a problem is detected at compile time moviesDS.map(m => m.movie_title - m.movie_title)
    val moviesArray: Array[Movie] = moviesDS.take(5)

    moviesArray.foreach(println)

    //        For those who use the Scala programming language on a regular basis, working
    //with the strongly typed Dataset APIs will feel natural and give you impression that those
    //objects in the Dataset reside locally.
    //When you use the strongly typed Dataset APIs, Spark implicitly converts each Row
    //instance to the domain-specific object that you provide. This conversion has some cost
    //in terms of performance; however, it provides more flexibility.
    //One general guideline to help with deciding when to use a DataSet over a DataFrame
    //is the desire to have a higher degree of type safety at compile time
  }
}