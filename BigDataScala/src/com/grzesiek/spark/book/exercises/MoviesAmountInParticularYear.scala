package com.grzesiek.spark.book.exercises
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object MoviesAmountInParticularYear {
  
  
  def main(args: Array[String]) {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      def extracYearAndName(line: String) {
        
        val elements = line.split("\\t")
        (elements(1), elements(2))
      }
      
      val sc = new SparkContext("local[*]", "MoviesAmountInParticularYear")
      
      
      
      val lines = sc.textFile("../data/beginning-apache-spark-2-master/chapter3/data/movies/movie-ratings.tsv")
      
      val yearAndName = lines.map(x => (x.split("\t")(2).toInt, 1))
      
      //val yearAndName = lines.map(extracYearAndName)
      
      val solution = yearAndName.reduceByKey((total, value) => total + value)
      
      //yearAndName.foreach(println)
      val sortedSolution = solution.sortByKey()
      sortedSolution.collect().foreach(println)
      //lines.foreach(println)
    
  }
}