package com.spark.sql.aggregations.with_grouping
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationWithPivoting extends App {

  //  Pivoting is a way to summarize the data by specifying one of the categorical columns
  //and then performing aggregations on another columns such that the categorical
  //values are transposed from rows into individual columns. Another way of thinking
  //about pivoting is that it is a way to translate rows into columns while applying one or
  //more aggregations. This technique is commonly used in data analysis or reporting.
  //The pivoting process starts with the grouping of one or more columns, then pivots on
  //a column, and finally ends with applying one or more aggregations on one or more
  //columns. Listing 5-17 shows a pivoting example on a small dataset of students where
  //each row contains the student name, gender, weight, and graduation year. You would
  //like to know the average weight of each gender for each graduation year.

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  case class Student(name: String, gender: String, weight: Int, graduation_year: Int)

  val studentsDF = Seq(
    Student("John", "M", 180, 2015),
    Student("Mary", "F", 110, 2015),
    Student("Derek", "M", 200, 2015),
    Student("Julie", "F", 109, 2015),
    Student("Allison", "F", 105, 2015),
    Student("kirby", "F", 115, 2016),
    Student("Jeff", "M", 195, 2016)).toDF

  studentsDF.groupBy("graduation_year").pivot("gender").avg("weight").show()

  //    The previous example has one aggregation, and the gender categorical column has
  //only two possible unique values; therefore, the result table has only two columns. If the
  //gender column has three possible unique values, then there will be three columns in
  //the result table. You can leverage the agg function to perform multiple aggregations,
  //which will create more columns in the result table. See Listing 5-18 for an example of
  //performing multiple aggregations on the same DataFrame as in Listing 5-17.

  studentsDF.groupBy("graduation_year").pivot("gender").agg(

    min("weight").as("min"),
    max("weight").as("max"),
    avg("weight").as("avg")).show()

  //    The number of columns added after the group columns in the result table is
  //the product of the number of unique values of the pivot column and the number of
  //aggregations.
  //If the pivoting column has a lot of distinct values, you can selectively choose which
  //values to generate the aggregations for. Listing 5-19 shows how to specify values to the
  //pivoting function.

  studentsDF.groupBy("graduation_year").pivot("gender", Seq("M"))
    .agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")).show()
      
//      Specifying a list of distinct values for the pivot column actually will speed up the
//pivoting process. Otherwise, Spark will spend some effort in figuring out a list of distinct
//values on its own.

}