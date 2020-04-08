package com.spark.sql.joins
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LeftAndRightOuterJoin extends App {

  //  Left Outer Joins
  //The joined dataset of this join type includes all the rows from an inner join plus all
  //the rows from the left dataset that the join expression evaluates to false. For those
  //nonmatching rows, it will fill in a NULL value for the columns of the right dataset.

  //  Right Outer Joins
  //The behavior of this join type resembles the behavior of the left outer join type, except
  //the same treatment is applied to the right dataset. In other words, the joined dataset
  //includes all the rows from an inner join plus all the rows from the right dataset that
  //the join expression evaluates to false. For those nonmatching rows, it will fill in a NULL
  //value for the columns of the left dataset.

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  case class Employee(first_name: String, dept_no: Long)
  val employeeDF = Seq(
    Employee("John", 31),
    Employee("Jeff", 33),
    Employee("Mary", 33),
    Employee("Mandy", 34),
    Employee("Julie", 34),
    Employee("Kurt", null.
      asInstanceOf[Int])).toDF

  case class Dept(id: Long, name: String)
  val deptDF = Seq(
    Dept(31, "Sales"),
    Dept(33, "Engineering"),
    Dept(34, "Finance"),
    Dept(35, "Marketing")).toDF
  // register them as views so we can use SQL for perform joins
  employeeDF.createOrReplaceTempView("employees")
  deptDF.createOrReplaceTempView("departments")

  val joinExpression = employeeDF.col("dept_no") === deptDF.col("id")

  employeeDF.join(deptDF, 'dept_no === 'id, "left_outer").show

  // using SQL
  spark.sql("select * from employees LEFT OUTER JOIN departments on dept_no == id").show

  employeeDF.join(deptDF, 'dept_no === 'id, "right_outer").show
  // using SQL
  spark.sql("select * from employees RIGHT OUTER JOIN departments on dept_no == id").show

  //Outer Joins (aka Full Outer Joins)
  //The behavior of this join type is effectively the same as combining the result of both
  //the left outer join and the right outer join.

  employeeDF.join(deptDF, 'dept_no === 'id, "outer").show
  // using SQL
  spark.sql("select * from employees FULL OUTER JOIN departments on dept_no == id").show
}