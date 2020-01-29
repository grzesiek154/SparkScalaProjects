package com.spark.dataframes
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object CreatingDataFramesByReadingORCFiles {
  
  
//  Optimized Row Columnar (ORC) is another popular open source self-describing
//columnar storage format in the Hadoop ecosystem. It was created by a company called
//Cloudera as part of the initiative to massively speed up Hive. It is quite similar to Parquet
//in terms of efficiency and speed and was designed for analytics workloads. Working with
//ORC files is just as easy as working with Parquet files
//  
   def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      
       val movies = spark.read.orc("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.orc")
       
       movies.printSchema()
       
       movies.show(10)
      
      
      
      
    }
}