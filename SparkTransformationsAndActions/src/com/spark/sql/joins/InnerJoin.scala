package com.spark.sql.joins
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object InnerJoin extends App {

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

  employeeDF.join(deptDF, joinExpression).show()
  employeeDF.join(deptDF, joinExpression, "inner").show()

  // using SQL
  spark.sql("select * from employees JOIN departments on dept_no == id").show

  //  As expected, the joined dataset contains only the rows with matching department
  //IDs from both the employee and department datasets and the columns from both
  //datasets. The output tells you exactly which department each employee belongs to.
  //The join expression can be specified inside the join transformation or using the
  //where transformation. It is possible to refer to the columns in the join expression using
  //a short-handed version if the column names are unique. If not, then it is required to
  //specify which DataFrame a particular column comes from by using the col function.

  // a shorter version of the join expression
  employeeDF.join(deptDF, 'dept_no === 'id).show
  // specify the join expression inside the join transformation
  employeeDF.join(deptDF, employeeDF.col("dept_no") === deptDF.col("id")).show
  // specify the join expression using the where transformation
  employeeDF.join(deptDF).where('dept_no === 'id).show
}