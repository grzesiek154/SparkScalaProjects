package com.grzesiek.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object CustomerAmountSpent {
  
  
      def parseLine(line: String) = {
        
        val fields = line.split(",")
        val customerId = fields(0).toInt
        val amount = fields(2).toFloat
        
        (customerId, amount)
      }
  
  
    def main(args: Array[String]) {
      
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "CustomerAmountSepnt")
    
    val lines = sc.textFile("../customer-orders.csv")
    
    val rdd = lines.map(parseLine)
    
    //rdd.foreach(println)
    
    val totalAmountByClinetId = rdd.reduceByKey((x,y) => (x + y)).sortByKey()
    
    val results = totalAmountByClinetId.collect()
    
    val reverseTuple = totalAmountByClinetId.map(x => (x._2, x._1)).sortByKey()
    
    val results2 = reverseTuple.collect()
    
    //results.foreach(println)
    
//      for (result <- results) {
//      val customerId = result._1
//      val amount = result._2
//      println(s"customer with id: $customerId spent:  $amount")
//    }
    
        for (result <- results2) {
      val customerId = result._2
      val amount = result._1
      println(s"customer with id: $customerId spent:  $amount")
    }
    
     //reverseTuple.foreach(println)
    
    }
}