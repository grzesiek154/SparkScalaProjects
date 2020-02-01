package com.spark.dataframes.operations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.Seq

object Introduction {

  //  Working with Columns
  //Most of the DataFrame structured operations in Table 4-7 will require you to specify one
  //or more columns. For some of them, the columns are specified in the form of a string,
  //and for some the columns need to be specified as instances of the Column class. It is
  //completely fair to question why there are two options and when to use what. To answer
  //those questions, you need to understand the functionality the Column class provides. At a
  //high level, the functionalities that the Column class provides can be broken down into the
  //following categories:
  //• Mathematical operations such as addition, multiplication, and so on
  //• Logical comparisons between a column value or a literal such as
  //equality, greater than, less than, and so on
  //• String pattern matching such as like, starting with, ending with, and
  //so on
  //For a complete list of available functions in the Column class, please refer to the
  //Scala documentation at https://spark.apache.org/docs/latest/api/scala/index.
  //html#org.apache.spark.sql.Column.
  //With an understanding of the functionality that the Column class provides, you can
  //conclude that whenever there is a need to specify some kind of column expression, then
  //it is necessary to specify the column as an instance of the Column class rather than a
  //string. The upcoming examples will make this clear.

  //  Different Ways of Referring to a Column
  //Way Example Description
  //"" "columName" this refers to a column as a string type.
  //col col("columnName") the col function returns an instance of the
  //Column class.
  //column column("columnName") Similar to col, this function returns an instance
  //of the Column class.
  //$ $"columnName" this is a syntactic sugar way of constructing a
  //Column class in Scala.
  //' (tick mark) 'columnName this is a syntactic sugar way of constructing a
  //Column class in Scala by leveraging the Scala
  //symbolic literals feature.

  //  The col and column functions are synonymous, and both are available in the Scala
  //and Python Spark APIs. If you often switch between the Spark Scala and Python APIs,
  //then it makes sense to use the col function so there is a consistency in your code. If you
  //mostly or exclusively use the Spark Scala APIs, then my recommendation is to use ' (tick
  //mark) because there is only a single character to type. The DataFrame class has its own
  //col function, which is used to disambiguate between columns with the same name from
  //two or more DataFrames when performing a join. Listing 4-21 provides examples of
  //different ways to refer to a column

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    //Different Ways of Referring to a Column
    val kvDF = Seq((1, 2), (2, 3)).toDF("key", "value")

    kvDF.select("key")
    kvDF.select(col("key"))

    kvDF.select(column("key"))
    kvDF.select($"key")
    kvDF.select('key)
    // using the col function of DataFrame
    kvDF.select(kvDF.col("key"))

    kvDF.select('key, 'key > 1).show

  }
}