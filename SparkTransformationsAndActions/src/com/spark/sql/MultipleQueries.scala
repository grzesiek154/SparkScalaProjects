package com.spark.sql
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object MultipleQueries {
  
  
   def main(args: Array[String]) {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
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
     
     
     val infoDF = spark.sql("select current_date() as today , 1 + 100 as value")
     
     // select from a view
      spark.sql("select * from movies where actor_name like '%Jolie%' and produced_year > 2009").show
      
      // mixing SQL statement and DataFrame transformation
      spark.sql("select actor_name, count(*) as count from movies group by actor_name").where('count > 30).orderBy('count.desc).show
      
      // using a subquery to figure out the number movies were produced in each year.
      // leverage """ to format multi-line SQL statement
      
      spark.sql("""select produced_year, count(*) as count from (select distinct movie_title, produced_year from movies) group by produced_year""").orderBy('count.desc).show(5)
      
      // select from a global view requires prefixing the view name with key word 'global_temp'
      spark.sql("select count(*) from global_temp.movies_g").show
      
//      Instead of reading the data file through DataFrameReader and then registering the
//newly created DataFrame as a temporary view, it is possible to issue a SQL query directly
//from a file. 
       spark.sql("SELECT * FROM parquet.`../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet`").show(5)
   }
}