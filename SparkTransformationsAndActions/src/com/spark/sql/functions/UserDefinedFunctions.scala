package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.functions.{col, udf}

case class Student(name: String, score: Int)

object UserDefinedFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val studentDF = Seq(
    Student("Joe", 85),
    Student("Jane", 90),
    Student("Mary", 55)).toDF()

  // register as a view
  studentDF.createOrReplaceTempView("students")

  // create a function to convert grade to letter grade
  def letterGrade(score: Int): String = {

    score match {
      case score if score > 100 => "Cheating"
      case score if score >= 90 => "A"
      case score if score >= 90 => "B"
      case score if score >= 90 => "C"
      case _                    => "F"
    }
  }
  
    val letterGrade2 = (score: Int) => {

    score match {
      case score if score > 100 => "Cheating"
      case score if score >= 90 => "A"
      case score if score >= 90 => "B"
      case score if score >= 90 => "C"
      case _                    => "F"
    }
  }

  // register as a UDF:
  
  //Call the UDF in Spark SQL
  val letterGradeSQL = spark.udf.register("letterGradeSQL",letterGrade2)
  
  //Use UDF with DataFrames
  val letterGradeUDF = udf(letterGrade(_:Int):String)

  studentDF.select($"name", $"score", letterGradeUDF($"score").as("grade")).show()
  spark.sql("SELECT name, letterGradeSQL(score) as grade FROM students").show()

}