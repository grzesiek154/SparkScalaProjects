package com.spark.datawriting
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DataPartitioning {
  
  
    def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
      movies.write.format("csv").mode("overwrite").option("sep",",").save("temp/output/csv")
      
//      The idea of writing data out using partitioning and bucketing is borrowed from
//the Apache Hive user community. As a general rule of thumb, the partition by column
//should have low cardinality. In the movies DataFrame, the produced_year column is
//a good candidate for the partition by column. Let’s say you are going to write out the
//movies DataFrame with partitioning by the produced_year column. DataFrameWriter
//will write out all the movies with the same produced_year into a single directory. The
//number of directories in the output folder will correspond to the number of years in the
//movies DataFrame. 
      
      
      
      
      
      movies.write.partitionBy("produced_year").save("tmp/output/movies")
      
      val numOfPartitions = movies.rdd.getNumPartitions
      println(numOfPartitions)
    
    // the /tmp/output/movies directory will contain the following subdirectories
//produced_year=1961 to produced_year=2012
//The directory names generated by the partitionBy option seems strange because
//each directory name consists of the partitioning column name and the associated value.
//These two pieces of information are used at data reading time to choose which directory
//to read based on the data access pattern, and therefore it ends up reading much less data
//than otherwise.
      
    }
}