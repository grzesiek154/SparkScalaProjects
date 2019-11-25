package com.spark.transformations

import org.apache.spark.SparkContext

object CommonTransformations {
  
  
  def main (args: Array[String]) {
    
    val stringList = Array("Spark is awesome","Spark is cool")
    val sc = new SparkContext("local[*]", "CommonTransformations")
    val stringRDD = sc.parallelize(stringList)
    
    
//    The second most commonly used transformation is flatMap. Let’s say you want to
//transform the stringRDD from a collection of strings to a collection of words. The
//flatMap transformation is perfect for this use case
    val wordRDD = stringRDD.flatMap(line => line.split(" "))
    wordRDD.collect().foreach(println)
    
    
//    Another commonly used transformation is the filter transformation. It does what its
//name sounds like, which is to filter a dataset down to the rows that meet the conditions
//defined inside the given func.
//A simple example is to find out how many lines in the stringRDD contain the word
//awesome. 
    val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
    awesomeLineRDD.collect
    
  }
}