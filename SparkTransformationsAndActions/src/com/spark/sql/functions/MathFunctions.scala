package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MathFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val numberDF = Seq(3.14159, 4.8, 2018).toDF("pie", "gpa", "year")

  numberDF.select(round('pie).as("pie0"), round('pie, 1).as("pie1"), round('pie, 2).as("pie2"),
    round('gpa).as("gpa"),
    round('year).as("year"))
    .show
  // because it is a half-up rounding, the gpa value is rounded up to 4.0

}