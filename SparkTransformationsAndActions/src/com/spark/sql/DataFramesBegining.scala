package com.spark.sql
import org.apache.spark.SparkContext
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.log4j._

object DataFramesBegining {
  
  
  def main (args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
//    val spark = new SparkContext("local[*]","DataFramesBegining")
//    val sqlContext = new SQLContext(spark)
//    val rdd = spark.parallelize(1 to 10).map(x => (x, Random.nextInt(100)*x))
//    
//    val kvDF = sqlContext.createDataFrame(rdd).toDF("key", "val")
    
    val spark = SparkSession.builder
  .master("local[*]")
  .appName("DataFramesBegining")
  .config("spark.sql.warehouse.dir", "target/spark-warehouse")
  .getOrCreate()
  
   val rdd = spark.sparkContext.parallelize(1 to 10).map(x => (x, Random.nextInt(100)*x))
   val dataFromRdd = spark.createDataFrame(rdd).toDF()
   
   dataFromRdd.show()
   
  }
}