package com.spark.datawriting
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Introduction {
  
  
    def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
//      One of the important options in the DataFrameWriter class is the save mode, which
//controls how Spark will handle the situation when the specified output folder already
//exists. 
      
//      append - this appends the dataFrame data to the list of files that already exist at
//the specified destination location.
//overwrite - this completely overwrites any data files that already exist at the
//specified destination location with the data in the dataFrame.
//error
//errorIfExists
//default - this is the default mode. if the specified destination location exists, then
//DataFrameWriter will throw an error.
//ignore - if the specified destination location exists, then simply do nothing. in other
//words, silently don’t write out the data in the dataFrame
      
      // write data out as CVS format, but using a '#' as delimiter
      //movies.write.format("csv").option("sep", "#").save("temp/output/csv")
      
      // write data out using overwrite save mode
      movies.write.format("csv").mode("overwrite").option("sep",",").save("temp/output/csv")
      
      val numOfPartitions = movies.rdd.getNumPartitions
      
      println(numOfPartitions)
      
      
      
    }
}