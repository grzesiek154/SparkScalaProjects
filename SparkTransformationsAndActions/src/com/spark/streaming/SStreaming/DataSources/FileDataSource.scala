package com.spark.streaming.SStreaming.DataSources

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

//The File data source is the simplest to understand and work with. Let’s say there is a
//need to process new files that are periodically copied into a directory. This is the perfect
//data source for this use case. Out of the box, it supports all the commonly used file
//formats including text, CSV, JSON, ORC, and Parquet. For a complete list of supported
//file formats, please consult the DataStreamReader interface. Among the four options that
//the File data source supports, only the input directory to read files from is required.
//As new files are copied into a specified directory, the File data source will pick up
//all of them for processing. It is possible to configure the File data source to selectively
//pick up only a fixed number of new files for processing. The option to use to specify the
//number of files is the maxFilesPerTrigger option

//Another interesting optional option that the File data source supports
//is whether to process the latest files before the older files. The last timestamp of a file
//is used to determine which file is newer than another file. The default behavior is to
//process files from oldest to latest. This particular option is useful when there is a large
//backlog of files to process and you want to process the new files first.

object FileDataSource extends App {

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

    // if we want to specify maxFilesPerTrigger
  val mobileSSDF1 = spark.readStream.schema(mobileDataSchema).option("maxFilesPerTrigger", 5)
    .json("../data/beginning-apache-spark-2-master/chapter6/data/mobile/input")

    // if we want to process new files first
  val mobileSSDF2 = spark.readStream.schema(mobileDataSchema).option("latestFirst", true)
    .json("../data/beginning-apache-spark-2-master/chapter6/data/mobile/input")

}