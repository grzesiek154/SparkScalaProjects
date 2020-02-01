package com.spark.dataframes
import org.apache.log4j._
import java.sql.DriverManager
import org.apache.spark.sql.SparkSession

object CreatingDataFramesFromJDBC {
  
   def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
       val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFramesFromJDBC")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      
      val mysqlURL= "jdbc:mysql://localhost:3306/sakila"
      val filmDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver")
      .option("url", mysqlURL)
      .option("dbtable", "film")
      .option("user", "root")
      .option("password","root")
      .load()
      
       filmDF.printSchema()
       
//       When working with a JDBC data source, Spark pushes the filter conditions all the way
//down to the RDBMS as much as possible. By doing this, much of the data will be filtered
//out at the RDBMS level, and therefore this will not only speed up the data filtering logic
//but dramatically reduce the amount of data Spark needs to read. This optimization
//technique is known as predicate pushdown, and Spark will often do this when it knows a
//data source can support the filtering capability. Parquet is another data source that has
//this capability. The “Catalyst Optimizer” section in chapter 5 will provide an example of
//what this looks like.
   }
  
}