package com.spark.transformations

import org.apache.log4j._
import org.apache.spark.SparkContext

object TransformationsOne {
  
 
  
  def getVixOpen(line: String) = {
    
    val fields = line.split(",")
    
    val date = fields(0).split("-")
    //val dateSplited = date.split("-")
    val year = date(0).toInt
    val vixOpen = fields(1).toDouble
    
    (year, vixOpen)
  }
  
  def main(args: Array[String]) {
    
  Logger.getLogger("org").setLevel(Level.ERROR)  
  
  val sc = new SparkContext("local[*]", "TransformationsOne")
  val lines = sc.textFile("../data/vix-daily.csv")
  val filtredLines = lines.filter(line => !line.contains("Date"))
  val rdd = filtredLines.map(line => getVixOpen(line))
  //val rddSorted = rdd.sortByKey()
  //val rddSwaped = rdd.map(x => (x._2, x._1)).sortByKey()
  
  val vixSumForYear = rdd.reduceByKey((total, value) => total + value)
  val vixSumForYearSorted = vixSumForYear.sortByKey()
  
   
   //rdd.collect.foreach(println)
  
  vixSumForYearSorted.collect.foreach(println)
   }
  
  
}