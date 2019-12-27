package com.spark.transformations

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.util.Random

object CommonTransformations {
  
def main (args: Array[String]) {
  
  
    Logger.getLogger("org").setLevel(Level.ERROR) 
     
    val stringList = Array("Spark is awesome","Spark is cool")
    val sc = new SparkContext("local[*]", "CommonTransformations")
    val stringRDD = sc.parallelize(stringList)
    
// flatMap()    
//    The second most commonly used transformation is flatMap. Let’s say you want to
//transform the stringRDD from a collection of strings to a collection of words. The
//flatMap transformation is perfect for this use case
    val wordRDD = stringRDD.flatMap(line => line.split(" "))
    println("FlatMap transformation")
    wordRDD.collect().foreach(println)
    
// filter()    
//    Another commonly used transformation is the filter transformation. It does what its
//name sounds like, which is to filter a dataset down to the rows that meet the conditions
//defined inside the given func.
//A simple example is to find out how many lines in the stringRDD contain the word
//awesome. 
    val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
    println("Filter transformation")
    awesomeLineRDD.collect().foreach(println)
    
 //mapPartitionWithIndex and mapPartition   
//    One small difference between the mapPartitionWithIndex and mapPartition
//transformations is that the partition number is available to the former transformation.
//In short, the mapPartitions and mapPartitionsWithIndex transformations are used
//to optimize the performance of your data processing logic by reducing the number of
//times the expensive setup step is called.
    
    val sampleList = Array("One", "Two", "Three", "Four","Five")
    val sampleRDD = sc.parallelize(sampleList, 2)
//    val result = sampleRDD.mapPartitions((itr:Iterator[String]) => {
//                val rand = new Random(System.currentTimeMillis +
//                Random.nextInt)
//                itr.map(l => l + ":" + rand.nextInt)
//    })
    
   
    
    
     def addRandomNumber(rows:Iterator[String]) = {
      val rand = new Random(System.currentTimeMillis + Random.nextInt)
      rows.map(l => l + " : " + rand.nextInt)
      
    }
    
    val result = sampleRDD.mapPartitions((rows: Iterator[String]) => addRandomNumber(rows))
    
    println("MapPartition transformation")
    result.collect.foreach(println)
    
  }

}