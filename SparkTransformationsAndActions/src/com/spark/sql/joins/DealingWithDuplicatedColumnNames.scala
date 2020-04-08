package com.spark.sql.joins
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DealingWithDuplicatedColumnNames extends App {
  
//  Sometimes there is an unexpected issue that comes up after joining two DataFrames
//with one or more columns that have the same name. When this happens, the joined
//DataFrame would have multiple columns with the same name. In this situation, it is not
//easy to refer to one of those columns while performing some kind of transformation on
//the joined DataFrame. 
  
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
  
  val deptDF2 = deptDF.withColumn("dept_no", 'id)
  
  //Spark SQL functions lit() and typedLit() are used to add a new column by assigning a literal or constant value to Spark DataFrame. Both functions return Column as return type. 
  val deptDF3 = deptDF.withColumn("asd", lit("test"))
  
val dupNameDF = employeeDF.join(deptDF2, employeeDF.col("dept_no") === deptDF2.col("dept_no"))

 dupNameDF.printSchema()
 
// dupNameDF.select("dept_no")
 
// will throw org.apache.spark.sql.AnalysisException: Reference 'dept_no' is ambiguous,
//could be: dept_no#30L, dept_no#1050L.;
 
// Use the Original DataFrame
//The joined DataFrame remembers which columns come from which original DataFrame
//during the joining process. To disambiguate which DataFrame a particular column
//comes from, you can just tell Spark to prefix it with its original DataFrame name. 
 
 dupNameDF.select(deptDF2.col("dept_no"))
 
// Renaming Column Before Joining
//To avoid the previous column name ambiguity issue, another approach is to rename a
//column in one of the DataFrames using the withColumnRenamed transform. Since this is
//simple, I will leave it as an exercise for you.
 
  val deptDF4 = deptDF2.withColumnRenamed("dept_no", "dept_no_origin")
  deptDF4.printSchema()
  val dupNameDF2 = employeeDF.join(deptDF4, employeeDF.col("dept_no") === deptDF4.col("dept_no_origin"))
  dupNameDF2.show
  
// Using a Joined Column Name
//In the case when the joined column name is the same in both DataFrames, you can
//leverage a version of the join transformation that automatically removes the duplicate
//column name from the joined DataFrame. However, if this were a self-join, meaning
//joining a DataFrame to itself, then there is no way to refer to other duplicate column
//names. In that case, you would need to use the first technique to rename the columns
//of one of the DataFrames. 
  
  val noDupNameDF = employeeDF.join(deptDF2, "dept_no")
  
}