package com.grzesiek.spark.copy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.tools.scalap.Main

object FriendsByName {
  
  def parseLine(line: String) = {
    
    val fields = line.split(",")
    
      val name = fields(1).toString()
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (name, numFriends)
   
  }
  
  
  def main(args: Array[String]) {
  
        // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
      // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByName")
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")
    
      // Use our parseLines function to convert to (name, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    
    val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    val averageByName = totalsByName.mapValues(x => x._1 / x._2)
    
    
    val testResult = rdd.mapValues(x => (x, 1))
    val results = averageByName.collect()
    
     // Sort and print the final results.
    results.sorted.foreach(println)
   // totalsByName.foreach(println)
  
  }
  
  
}