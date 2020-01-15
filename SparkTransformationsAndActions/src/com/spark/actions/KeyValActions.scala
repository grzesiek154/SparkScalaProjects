
package com.spark.actions

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object KeyValActions {
  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
//    countByKey( )
//For a given pair RDD, this action ignores the value of each row and reports only the number
//of values with the same key for each key to the driver.
    
    val sc = new SparkContext("local[*]", "KeyValActions")
    
    val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
                  ("candy1", 2.0), ("candy3", 6.0)))
    val countedCandyTx = candyTx.countByKey()
    
    countedCandyTx.foreach(println)
    
    
//    collectAsMap( )
//Similar to the collect action, this one brings the entire dataset to the driver side as a
//map, where each entry represents a row
    val candyTxCollect = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
                  ("candy1", 2.0), ("candy3", 6.0)))
    val collectedAsMapCandy = candyTxCollect.collectAsMap()
    println("!!!!!collectAsMap!!!!")
    collectedAsMapCandy.foreach(println)
//    Notice if the dataset contains multiple rows with the same key, it will be collapsed
//into a single entry in the map. There are four rows in the candyTx pair RDD; however,
//there are only three rows in the output. Two candy1 rows are collapsed into a single row.
    
    
//    lookup(key)
//This action can be used as a quick way to verify whether a particular key exists in the
//RDD. If there is more
//than one row with the same key, then the value of all those rows will be returned.
    
    val lookupCandy = candyTx.lookup("candy1")
    println("!!!!lookup!!!!!")
    lookupCandy.foreach(println)
  }

}