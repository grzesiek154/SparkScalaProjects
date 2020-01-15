package com.grzesiek.spark.book.exercises

import org.apache.spark.SparkContext
import org.apache.log4j._

object NumberOfMoviesEachActorAppear {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","NumberOfMoviesEachActorAppear")
    
    val data = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movies.tsv")
    
    val dataMap = data.map(x => (x.split("\t")(0), 1))
    
    val solution = dataMap.reduceByKey((total, value) => total + value)
    
    //dataMap.foreach(println)
    solution.foreach(println)
  }
  
  
}