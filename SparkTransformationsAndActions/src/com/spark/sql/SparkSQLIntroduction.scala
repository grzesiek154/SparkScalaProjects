package com.spark.sql
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLIntroduction {
  
//  Spark provides a few different ways to run SQL in Spark.
//• Spark SQL CLI (./bin/spark-sql)
//• JDBC/ODBC server
//• Programmatically in Spark applications
//This first two options provide an integration with Apache Hive to leverage the Hive
//metastore, which is a repository that contains the metadata and schema information
//about the various system and user-defined tables. This section will cover only the last
//option.
//DataFrames and Datasets are essentially like tables in a database. Before you can
//issue SQL queries to manipulate them, you need to register them as temporary views.
//Each view has a name, and that is what is used as the table name in the select clause.
//Spark provides two levels of scoping for the temporary views. One is at the Spark session
//level. When a DataFrame is registered at this level, only the queries that are issued in the
//same session can refer to that DataFrame. The session-scoped level will disappear when
//a Spark session is closed. The second scoping level is at the global level, which means
//these views are available to SQL statements in all Spark sessions. All the registered
//views are maintained in the Spark metadata catalog that can be accessed through
//SparkSession.
  
  
   def main(args: Array[String]) {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkSQLIntroduction")
      .getOrCreate()
      
    import spark.implicits._
    
    spark.catalog.listTables.printSchema()
    val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
    
    // now register movies DataFrame as a temporary view
    movies.createOrReplaceTempView("movies")
    spark.catalog.listTables.show
    
    // show the list of columns of movies view in catalog
    spark.catalog.listColumns("movies").show()
    
     // register movies as global temporary view called movies_g
     movies.createOrReplaceGlobalTempView("movies_g")
     
//     The previous example gives you a couple of views to select from. The programmatic
//way of issuing SQL queries is to use the sql function of the SparkSession class. Inside the
//SQL statement, you have access to all SQL expressions and built-in functions. Once the
//SparkSession.sql function executes the given SQL query, it will return a DataFrame. The
//ability to issue SQL statements and use DataFrame transformations and actions provides
//you with a lot of flexibility in how you choose to perform distributed data processing in
//Spark
   }
}