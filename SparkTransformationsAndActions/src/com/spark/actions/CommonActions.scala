package com.spark.actions

import org.apache.log4j._
import org.apache.spark.SparkContext

object CommonActions {
  
  
  
  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = new SparkContext("local[*]", "CommonActions")
    val numberRDD = spark.parallelize(List(1,2,3,4,5,6,7,8,9,10), 2)
    
    //collect
//    This is a fairly easy-to-understand action because it does exactly what it sounds like.
//It collects all the rows from each of the partitions in an RDD and brings them over to
//the driver program. If your RDD contains 100 million rows, then it is not a good idea to
//invoke the collect action because the driver program most likely doesn’t have sufficient
//memory to hold all those rows. As a result, the driver will most likely run into an out-ofmemory error and your Spark application or shell will die. This action is typically used
//once the RDD is filtered down to a smaller size that can fit the memory size of the driver
//program
    println("collect action")
    numberRDD.collect.foreach(println)
    
    
    
//    count( )
//Similar to the collect action, this action does exactly what it sounds like. It returns the
//number of rows in an RDD by getting the count from all partitions and finally sums them up.
//See Listing 3-34 for an example of using the count action
    val numberOfValues = numberRDD.count()
    println("count action")
    println(numberOfValues)
    
//    first( )
//This action returns the first row in an RDD. Now you may be wondering, what does the
//first row mean? Is there any ordering involved? It turns out it literally means the first row
//in the first partition. However, be careful about calling this action if your RDD is empty.
//In that case, this action will throw an exception. See Listing 3-36 for an example of using
//this action
    
    val firstRow = numberRDD.first()
    println("first action")
    println(firstRow)
    
//take(n)
//This action returns the first n rows in the RDD by collecting rows from the first partition
//and then moves to the next partition until the number of rows matches n or the last
//partition is reached. If n is larger than the number of rows in the dataset, then it will
//return all the rows. take(1) is equivalent to the first() action    
    
  }
}