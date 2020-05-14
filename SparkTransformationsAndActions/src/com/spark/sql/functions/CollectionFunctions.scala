package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CollectionFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("CollectionFunctions")
    .getOrCreate()

  import spark.implicits._

  val tasksDF = Seq(("Monday", Array("Pick Up John", "Buy Milk", "PayBill"))).toDF("day", "tasks")

  //1.get the size of the array, sort it, and check to see if a particular
  //value exists in the array
  tasksDF.select('day, size('tasks).as("sorted_tasks"), sort_array('tasks).as("sorted_tasks"),
    array_contains('tasks, "Pay Bill").as("shouldPayBill")).show()

  //2.the explode function will create a new row for each element in the array
  tasksDF.select('day, explode('tasks)).show

  val todos = """{"day": "Monday","tasks": ["Pick Up John","Buy Milk","PayBill"]}"""
  val todoStrDF = Seq(todos).toDF("todos_str")
  todoStrDF.printSchema()

  //3. in order to convert a JSON string into a Spark struct data type, we need to describe its structure to Spark
  val todoSchema = new StructType().add("day", StringType).add("tasks", ArrayType(StringType))

  // use from_json to convert JSON string
  val todosDF = todoStrDF.select(from_json('todos_str, todoSchema).as("todos"))

  todosDF.printSchema()

  // retrieving value out of struct data type using the getItem function of Column class
  todosDF.select(
    'todos.getItem("day"),
    'todos.getItem("tasks"),
    'todos.getItem("tasks").getItem(0).as("first_task")).show(false)
    
   // to convert a Spark struct data type to JSON string, we can use to_json function
    
    todosDF.select(to_json('todos)).show()

}