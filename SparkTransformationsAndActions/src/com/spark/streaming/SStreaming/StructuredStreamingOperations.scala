package com.spark.streaming.SStreaming
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

//you can use most of the familiar operations and Spark SQL functions to express your application streaming
//computation logic. However, it is important to note that not all operations in the
//DataFrame are supported for a streaming DataFrame. This is because some of them are
//not applicable in the context of streaming data processing. Examples of such operations
//include limit, distinct, and sort.
object StructuredStreamingOperations extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("StructuredStreamingOperations")
    .getOrCreate()

  import spark.implicits._

  val mobileDataDF = spark.read.json("../data/beginning-apache-spark-2-master/chapter6/data/mobile")
  val mobileDataSchema = new StructType().add("id", StringType, false)
    .add("action", StringType, false)
    .add("ts", TimestampType, false)

  val mobileSSDF = spark.readStream.schema(mobileDataSchema).json("../data/beginning-apache-spark-2-master/chapter6/data/mobile/input")
  val cleanMobileSSDF = mobileSSDF.filter($"action" === "open" || $"action"=== "close")
                                  .select($"id", upper($"action"), $"ts")
  val tableView = cleanMobileSSDF.createOrReplaceTempView("clean_mobile")
  val mobileQuery = spark.sql("select * from clean_mobile")
  val mobileConsoleSQ = mobileQuery.writeStream.format("console").option("truncate", "false")
    .outputMode("append").start()
    
  mobileConsoleSQ.awaitTermination()  
    

  //  It is important to note the following DataFrame transformations are not supported
  //yet in a streaming DataFrame either because they are too complex to maintain state or
  //because of the unbounded nature of streaming data.
  //• Multiple aggregations or a chain of aggregations on a streaming
  //DataFrame.
  //• Limit and take N rows.
  //• Distinct transformation. However, there is a way to deduplicate data
  //using a unique identifier.
  //• Sorting on a streaming DataFrame without any aggregation.
  //However, sorting is supported after some form of aggregation.
  //Any attempt to use one of the unsupported operations will result in an
  //AnalysisException exception and a message like “operation XYZ is not supported with
  //streaming DataFrames/Datasets.”

}