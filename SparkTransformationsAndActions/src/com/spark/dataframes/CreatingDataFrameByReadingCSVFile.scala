package com.spark.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

object CreatingDataFrameByReadingCSVFile {
  
  
//  Table 4-4. CSV Common Options
//Key Values Default Description
//sep - Single character , this is a single-character value used as a delimiter
//for each column.
//header - true, false false if the value is true, it means the first line in the file
//represents the column names.
//escape - any character \ this is the character to use to escape the character
//in the column value that is the same as sep.
//inferSchema - true, false false this specifies whether Spark should try to infer the
//column type based on column value.
  
//  Specifying the header and inferSchema options as true won’t require you to specify
//a schema. Otherwise, you need to define a schema by hand or programmatically create it
//and pass it into the schema function. If the inferSchema option is false and no schema is
//provided, Spark will assume the data type of all the columns to be the string type

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]) {
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      
      
     val movies = spark.read.option("header","true").csv("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.csv")
     
     val movieSchema = StructType(Array(StructField("actor_name", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", LongType, true)))
        
     val movies2 = spark.read.option("header","true").option("inferSchema","true").csv("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.csv")
     val movies3 = spark.read.option("header","true").schema(movieSchema).csv("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.csv")
     
     val movies4 = spark.read.option("header","true").option("sep", "\t").schema(movieSchema).csv("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.csv")
     
     
     
     movies4.show()
     //movies.printSchema()
     
    }
}