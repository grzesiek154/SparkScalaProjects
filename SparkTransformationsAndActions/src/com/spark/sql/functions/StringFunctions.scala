package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StringFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  //1. Different Ways of Transforming a String with Built-in String Functions
  val sparkDF = Seq((" Spark ")).toDF("name")
  // trimming
  sparkDF.select(trim('name).as("trim"), ltrim('name).as("ltrim"), rtrim('name).as("rtrim")).toDF.show

  //2. transform a string with concatenation, uppercase, lowercase and reverse
  val sparkAwesomeDF = Seq(("Spark", "is", "awesome")).toDF("subject", "verb", "adj")

  sparkAwesomeDF.select(concat_ws(" ", 'subject, 'verb, 'adj).as("sentence"))
    .select(
      lower('sentence).as("lower"),
      upper('sentence).as("upper"),
      initcap('sentence).as("initcap"),
      reverse('sentence).as("reverse")).show

  //3. translate from one character to another
  sparkAwesomeDF.select('subject, translate('subject, "ar", "oc").as("translate")).show

  //  Regular expressions are a powerful and flexible way to replace some portion of a
  //string or extract substrings from a string. The regexp_extract and regexp_replace
  //functions are designed specifically for those purposes. Spark leverages the Java regular
  //expressions library for the underlying implementation of these two string functions.
  //The input parameters to the regexp_extract function are a string column, a pattern
  //to match, and a group index. There could be multiple matches of the pattern in a string;
  //therefore, the group index (starts with 0) is needed to identify which one. If there are no
  //matches for the specified pattern, this function returns an empty string. See Listing 5-39
  //for an example of working with the regexp_extract function.

  val rhymeDF = Seq(("A fox saw a crow sitting on a tree singing \"Caw! Caw! Caw!\"")).toDF("rhyme")
  // using a pattern
  rhymeDF.select(regexp_extract('rhyme, "[a-z]*o[xw]", 0).as("substring")).show
  
  rhymeDF.select(regexp_replace('rhyme, "fox|crow", "animal").as("new_rhyme")).show(false)
  rhymeDF.select(regexp_replace('rhyme, "[a-z]*o[xw]", "animal").as("new_rhyme")).show(false)
}