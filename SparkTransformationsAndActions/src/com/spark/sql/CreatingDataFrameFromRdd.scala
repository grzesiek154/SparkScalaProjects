package com.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.log4j._

object CreatingDataFrameFromRdd {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRdd")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val peopleRdd = spark.sparkContext.parallelize(Array(Row(1L, "John Doe", 30L), Row(2L, "Mary Jane", 25L)))

    val schema = StructType(Array(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", LongType, true)))
    val peopleDF = spark.createDataFrame(peopleRdd, schema)
    peopleDF.show()
  }
  
//  The ability to programmatically create a schema gives Spark applications the
//flexibility to adjust the schema based on some external configuration.
//Each StructField object has three pieces of information: name, type, and whether
//the value is nullable.
//The type of each column in a DataFrame is mapped to an internal Spark type, which
//can be a simple scalar type or a complex type. Table 4-1 describes the available internal
//Spark data types and associated Scala types.
//Creating DataFrames from a Range of Numbers
//Spark 2.0 introduced a new entry point for Spark applications. It is represented by
//a class called SparkSession, which has a convenient function called range that
//can easily create a DataFrame with a single column with the name id and the type
  
//Table 4-1. Spark Scala Types
//Data Type Scala Type
//BooleanType Boolean
//ByteType Byte
//ShortType Short
//IntegerType Int
//LongType Long
//FloatType Float
//DoubleType Double
//DecimalType java.math.BigDecial
//StringType String
//BinaryType Array[Byte]
//TimestampType java.sql.Timestamp
//DateType java.sql.Date
//ArrayType scala.collection.Seq
//MapType scala.collection.Map
//StructType org.apache.spark.sql.Row
}