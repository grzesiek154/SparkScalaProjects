package com.spark.dataframes
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object CreatingDataFrameByReadingParquestFiles {
  
//  Parquet is one of the most popular open source columnar storage formats in the Hadoop
//ecosystem, and it was created at Twitter. Its popularity is because it is a self-describing
//data format and it stores data in a highly compact structure by leveraging compressions.
//The columnar storage format is designed to work well with a data analytics workload
//where only a small subset of the columns are used during the data analysis. Parquet
//stores the data of each column in a separate file; therefore, columns that are not needed
//in a data analysis wouldn’t have to be unnecessarily read in. It is quite flexible when it
//comes to supporting a complex data type with a nested structure. Text file formats such
//as CVS and JSON are good for small files, and they are human-readable. For working
//with large datasets that are stored in long-term storage, Parquet is a much better file
//format to use to reduce storage costs and to speed up the reading step. If you take a peek
//at the movies.parquet file in the chapter4/data/movies folder, you will see that its size
//is about one-sixth the size of movies.csv.
//Spark works extremely well with the Parquet file format, and in fact Parquet is the
//default file format for reading and writing data in Spark. Since Parquet files are selfcontained, meaning the schema is stored inside the Parquet data file, it is easy to work
//with Parquet in Spark. 
  
    def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      
       val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
       
       movies.printSchema()
       
       movies.show(10)
      
      
      
      
    }
    
    
   
}