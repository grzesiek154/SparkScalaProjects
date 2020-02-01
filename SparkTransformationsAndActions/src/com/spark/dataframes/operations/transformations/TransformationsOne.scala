package com.spark.dataframes.operations.transformations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object TransformationsOne {
  
  
    def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      import spark.implicits._
      
      val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
      
//      select(columns)
//      This transformation is commonly used to perform projection, meaning selecting all
//      or a subset of columns from a DataFrame. During the selection, each column can be
//      transformed via a column expression. There are two variations of this transformation.
//      One takes the column as a string, and the other takes columns as the Column class. This
//      transformation does not permit you to mix the column type when using one of these two
//      variations. 
      
      movies.select("movie_title", "produced_year").show(5)
      
      movies.select('movie_title, ('produced_year - ('produced_year % 10)).as("produced_decade")).show(5)
      
//      The second example requires two column expressions: modulo and subtraction.
//Both them are implemented by the modulo (%) and subtraction (-) functions in the
//Column class (see the Scala documentation mentioned earlier). By default, Spark uses the
//column expression as the name of the result column. To make it more readable, the as
//function is commonly used to rename it to a more human-readable column name. As an
//astute reader, you can probably figure out the select transformation can be used to add
//one or more columns to a DataFrame.
      
      
      
      
//     selectExpr(expressions)
//This transformation is a variant of the select transformation. The one big difference
//is that it accepts one or more SQL expressions, rather than columns. However, both
//are essentially performing the same projection task. SQL expressions are powerful and
//flexible constructs to allow you to express column transformation logic in a natural way,
//just like the way you think. You can express SQL expressions in a string format, and Spark
//will parse them into a logical tree so they will be evaluated in the right order. Let’s say
//you want to create a new DataFrame that has all the columns in the movies DataFrame
//and introduce a new column to represent the decade a movie was produced in; then you
//would do something like
      
      movies.selectExpr("*","(produced_year - (produced_year % 10)) as decade").show(5)
      
      //The count function performs an aggregation over the entire DataFrame.
      
      movies.selectExpr("count(distinct(movie_title)) as movies", "count(distinct(actor_name)) as actors").show
    }
}