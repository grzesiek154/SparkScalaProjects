package com.practice
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordsCountInvertIndexTest {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val shakespeare = new File("../data/shakespeare")
  val sc = new SparkContext("local","WordsCountInvertIndex")
  val pathSeparator = File.separator

  val fileContent = sc.wholeTextFiles(shakespeare.toString).flatMap{
      
      case (location, "") => 
          Array.empty[((String, String), Int)]
      case (location, contents) =>
          val words = contents.split("""\W+""")
          val fileName = location.split("\\\\").last
          words.map(word => ((word, fileName), 1))
//          Pattern matching is eager. The first successful match in the order as written will win. If you reversed the order
//here, the case (location, "") would never match and the compiler would throw an "unreachable code"
//warning for it. czyli jesli pierwszy case wyszukla by nam wszystkie wartosci rowneiz te puste, drugi case
// nic by nie znalazl          
     
      }
    
    fileContent.foreach(println)

   
  }
}