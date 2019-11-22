package com.grzesiek.spark.playground

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object Superhero {
  
  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
  
  def splitNamesFromId(line: String) : Option[String] = {
    var elements = line.split('\"')
    
   if (elements.length > 1) {
      return Some(elements(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
  
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "Superhero")
    val names = sc.textFile("../marvel-names.txt")
    
    val lines = sc.textFile("../marvel-graph.txt")
   
    
    val superheroNames = names.flatMap(splitNamesFromId) 
    
    val something = superheroNames.map(x => (x,1)).reduceByKey( (x,y) => x + y)
    val another = something.map(x => (x._1, x._2)).sortByKey()
    
    
    val captinAmerica = superheroNames.filter(name => name.contains("Storm"))
    
    //captinAmerica.foreach(println)
    
    //superheroNames.foreach(println)
    names.foreach(println)
    
  
 }
}
