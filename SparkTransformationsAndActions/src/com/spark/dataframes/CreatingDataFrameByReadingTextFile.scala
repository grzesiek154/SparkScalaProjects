package com.spark.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object CreatingDataFrameByReadingTextFile {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]) {
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      
      
      var textFile = spark.read.text("someFile.txt")
      
      
      
      textFile.printSchema()
      textFile.show(5, false)
  }
}