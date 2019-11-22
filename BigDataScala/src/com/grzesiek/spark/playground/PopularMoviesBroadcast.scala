package com.grzesiek.spark.playground

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.log4j._
import org.apache.spark.SparkContext

object PopularMoviesBroadcast {
  
  
  def loadMoviesNames(): Map[Int, String] = {
    
      // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames :Map[Int, String] = Map()
    
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    //Console.println(lines)
        for (line <- lines) {
            var fields = line.split('|')
            if(fields.length > 1) {
              movieNames += (fields(0).toInt -> fields(1))
              
            }
        }
    return movieNames
    
  }
  
    def main(args: Array[String]) {
      
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "PopularMoviesBroadcast")
  
      val results = loadMoviesNames()
      
      results.foreach(println)
  
    }
}