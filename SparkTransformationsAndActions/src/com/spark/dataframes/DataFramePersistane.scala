package com.spark.dataframes
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object DataFramePersistane {
  
  
    def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .getOrCreate()
      import spark.implicits._
    
//    DataFrames can be persisted/cached in memory just like how it is done with RDDs. The
//same familiar persistence APIs (persist and unpersist) are available in the DataFrame
//class. However, there is one big difference when caching a DataFrame. Spark SQL knows
//the schema of the data inside a DataFrame, so it organizes the data in a columnar format
//as well as applies any applicable compressions to minimize space usage. The net result
//is it will require much less space to store a DataFrame in memory than storing an RDD
//when both are backed by the same data file
      
     

//Spark RDD persistence is an optimization technique in which saves the result of RDD evaluation. Using this we save the intermediate result so that we can use it further if required. It reduces the computation overhead.
//We can make persisted RDD through cache() and persist() methods. When we use the cache() method we can store all the RDD in-memory. We can persist the RDD in memory and use it efficiently across parallel operations.
//
//The difference between cache() and persist() is that using cache() the default storage level is MEMORY_ONLY while using persist() we can use various storage levels (described below). It is a key tool for an interactive algorithm. Because, when we persist RDD each node stores any partition of it that it computes in memory and makes it reusable for future use. This process speeds up the further computation ten times.
    
    val numDF = spark.range(1000).toDF("id")
    
    numDF.createOrReplaceTempView("num_df")
    
    // use Spark catalog to cache the numDF using name "num_df"
    spark.catalog.cacheTable("num_df")
    
    // force the persistence to happen by taking the count action
    numDF.count
    
    }
}