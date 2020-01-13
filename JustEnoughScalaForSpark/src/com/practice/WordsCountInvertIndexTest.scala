package com.grzesiek.spark.book.exercises
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordsCountInvertIndexTest {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val shakespeare = new File("../data/shakespeare")
  val sc = new SparkContext("local","WordsCountInvertIndex")
  val pathSeparator = File.separator
  //   val filePaths = fileContent.keys.map(content => content.split(pathSeparator + pathSeparator).last)
//   val filesContent = fileContent.values
  val fileContent = sc.wholeTextFiles(shakespeare.toString).flatMap{content =>
       val words =content._2.split("""\W+""")
       val fileName = content._1.split("\\\\")
       words.map(word => (word, 1))
     
             
    }
//    .reduceByKey((total, value) => total + value)
//    map { word_file_count_tup3 => (word_file_count_tup3._1._1, (word_file_count_tup3._1._2, word_file_count_tup3._2))}
    
    fileContent.foreach(println)
    //println(reduceByKeyTest)
//    println("===========================================================")
   
  }
}