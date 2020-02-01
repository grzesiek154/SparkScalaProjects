package com.spark.dataframes.operations.transformations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object DealingWithBadData {
  
      def main(args: Array[String]) {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      val badMovies = Seq(Row(null,null,2018L),
                          Row(null,null,null),
                          Row("John Doe","Aewsome Movies",null),
                          Row("Mary Jane", null, 2018L))
                          
      val badMoviesRdd = spark.sparkContext.parallelize(badMovies)
      val badMoviesDF = spark.createDataFrame(badMoviesRdd, movies.schema)
      
      badMoviesDF.show
      // dropping rows that have missing data in any column
      badMoviesDF.na.drop()
    // dropping rows that have missing data in any column
      badMoviesDF.na.drop("any")
      //dropping rows that have missing data in every single column
      badMoviesDF.na.drop("all").show()
      //drop row when column actor name has missing data
      badMoviesDF.na.drop(Array("actor_name")).show()
     
      }
}