package com.spark.dataframes.operations.transformations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TransformationsTwo {
  
  
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
//      filler(condition), where(condition)
//This transformation is a fairly straightforward one to understand. It is used to filter
//out the rows that don’t meet the given condition, in other words, when the condition
//evaluates to false. A different way of looking at the behavior of the filter transformation is
//that it returns only the rows that meet the specified condition. The given condition can
//simple or as complex as it needs to be. Using this transformation will require knowing
//how to leverage a few logical comparison functions in the Column class, like equality,
//less than, greater than, and inequality. Both the filter and where transformations have
//the same behavior, so pick the one you are most comfortable with. The latter one is just
//a bit more relational than the former. 
      
      movies.filter('produced_year < 2000)
      movies.where('produced_year > 2000).orderBy(asc("movie_title")).show
      // equality comparison require 3 equal signs
      movies.filter('produced_year === 2000).show(5)
      // inequality comparison uses an interesting looking operator =!=
      movies.select("movie_title","produced_year").filter('produced_year =!=2000).show(5)
      
      // to combine one or more comparison expressions, we will use either the OR and AND expression operator
      movies.filter('produced_year >= 2000 && length('movie_title) < 5).show(5)
      
      // the other way of accomplishing the same result is by calling the filterfunction two times
      movies.filter('produced_year >= 2000).filter(length('movie_title) < 5).show(5)
      
//      distinct, dropDuplicates
//      These two transformations have identical behavior. However, dropDuplicates allows
//you to control which columns should be used in deduplication logic. If none is specified,
//the deduplication logic will use all the columns in the DataFrame
      
      movies.select("movie_title").distinct.selectExpr("count(movie_title) asmovies").show
      movies.dropDuplicates("movie_title").selectExpr("count(movie_title) asmovies").show
      
//      sort(columns), orderBy(columns)
//Both of these transformations have the same semantics. The orderBy transformation is
//more relational than the other one. By default, the sorting is in ascending order, and it is
//fairly easy to change it to descending. When specifying more than one column, it is possible
//to have a different order for each of the columns. 
      
      val movieTitles = movies.dropDuplicates("movie_title").selectExpr("movie_title", "length(movie_title) as title_length", "produced_year")
      movieTitles.sort('title_length).show(5)
      
      // sorting in descending order
      movieTitles.orderBy('title_length.desc).show(5)  
      // sorting in descending order
      movieTitles.orderBy('title_length.desc).show(5)
      
      
  }
}