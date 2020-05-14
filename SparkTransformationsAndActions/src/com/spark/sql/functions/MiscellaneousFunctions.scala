package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Sometimes there is a need to generate monotonically increasing unique, but not
//necessarily consecutive, IDs for each row in the dataset. It is quite an interesting problem
//if you spend some time thinking about it. For example, if a dataset has 200 million rows
//and they are spread across many partitions (machines), how do you ensure the values
//are unique and increasing at the same time? This is the job of the monotonically_
//increasing_id function, which generates IDs as 64-bit integers. The key part in its
//algorithm is that it places the partition ID in the upper 31 bits

object MiscellaneousFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("CollectionFunctions")
    .getOrCreate()

  import spark.implicits._

  // first generate a DataFrame with values from 1 to 10 and spread them across 5 partitions
  val numDF = spark.range(1, 11, 2, 3)
  //range - Creates a Dataset with a single LongType column named id, containing elements in a range from start to end (exclusive) with a step value, with partition number specified.
  println(numDF.rdd.getNumPartitions)

  // now generate the monotonically increasing numbers and see which ones are in which partition
  numDF.select('id, monotonically_increasing_id().as("m_ii"), spark_partition_id().as("partition")).show

  //    If there is a need to evaluate a value against a list of conditions and return a value,
  //then a typical solution is to use a switch statement, which is available in most high-level
  //programming languages. When there is a need to do this with the value of a column in a
  //DataFrame, then you can use the when function for this use case.

  val dayOfWeek = spark.range(1, 8, 1)

  //convert each numerical value to a string
  dayOfWeek.select('id, when('id === 1, "Mon").when('id === 2, "Tue")
    .when('id === 3, "Wed")
    .when('id === 4, "Thu")
    .when('id === 5, "Fri")
    .when('id === 6, "Sat")
    .when('id === 7, "Sun").as("dow")).show

  // to handle the default case when we can use the otherwise function of the column class
  dayOfWeek.select('id, when('id === 6, "Weekend")
    .when('id === 7, "Weekend")
    .otherwise("Weekday").as("day_type")).show

  //    When working with data, it is important to handle null values properly. One of
  //the ways to do that is to convert them to some other values that represent null in your
  //data processing logic. Borrowing from the SQL world, Spark provides a function called
  //coalesce that takes one or more column values and returns the first one that is not null.
  //Each argument in the coalesce function must be of type Column, so if you want to fill in
  //a literal value, then you can leverage the lit function. The way this function works is it
  //takes a literal value as an input and returns an instance of the Column class that wraps
  //the input.

  // create a movie with null title
  case class Movie(actor_name: String, movie_title: String, produced_year: Long)
  val badMoviesDF = Seq(
    Movie(null, null, 2018L),
    Movie("John Doe", "Awesome Movie", 2018L)).toDF
    
    // use coalese to handle null value in title column
    badMoviesDF.select(coalesce('actor_name, lit("no_name")).as("new_title")).show

}