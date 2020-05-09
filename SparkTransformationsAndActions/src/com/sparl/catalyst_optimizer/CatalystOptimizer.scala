package com.sparl.catalyst_optimizer
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//At a high level, the Spark Catalyst translates the user-written data processing logic into a logical
//plan, then optimizes it using heuristics, and finally converts the logical plan to a physical
//plan. The final step is to generate code based on the physical plan
object CatalystOptimizer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.load("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
  
  // performi two filtering conditions
  val newMoviesDF = moviesDF.filter('produced_year > 1970).withColumn("produced_decade", 'produced_year + 'produced_year % 10)
  val testSelect = newMoviesDF.select('movie_title, 'produced_decade).where('produced_decade > 2010)
  
  testSelect.explain(true)
                    
 
}