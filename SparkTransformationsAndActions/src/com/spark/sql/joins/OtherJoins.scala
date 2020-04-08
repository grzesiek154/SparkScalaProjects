package com.spark.sql.joins
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OtherJoins extends App {

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
    Employee("Julie", 35),
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

  //  Left Anti-Joins
  //This join type enables you to find out which rows from the left dataset don’t have any
  //matching rows on the right dataset, and the joined dataset will contain only the columns
  //from the left dataset.

  employeeDF.join(deptDF, 'dept_no === 'id, "left_anti").show
  // using SQL
  spark.sql("select * from employees LEFT ANTI JOIN departments on dept_no == id").show

  //Left Semi-Joins
  //The behavior of this join type is similar to the inner join type, except the joined dataset
  //doesn’t include the columns from the right dataset. Another way of thinking about this
  //join type is its behavior is the opposite of the left anti-join, where the joined dataset
  //contains only the matching rows

  employeeDF.join(deptDF, 'dept_no === 'id, "left_semi").show
  // using SQL
  spark.sql("select * from employees LEFT SEMI JOIN departments on dept_no == id").show

  //  Cross (aka Cartesian)
  //In terms of usage, this join type is the simplest to use because the join expression is not
  //needed. Its behavior can be a bit dangerous because it joins every single row in the left
  //dataset with every row in the right dataset. The size of the joined dataset is the product
  //of the size of the two datasets. For example, if the size of each dataset is 1,024, then the
  //size of the joined dataset is more than 1 million rows. For this reason, the way to use this
  //join type is by explicitly using a dedicated transformation in DataFrame, rather than
  //specifying this join type as a string

  // using crossJoin transformation and display the count
  employeeDF.crossJoin(deptDF).count
  //Long = 24
  // using SQL and to display up to 30 rows to see all rows in the joined dataset
  spark.sql("select * from employees CROSS JOIN departments").show(30)
}