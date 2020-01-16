package com.grzesiek.spark.book.exercises
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.SQLContext

object WordsCountInvertIndexTest {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val shakespeare = new File("../data/shakespeare")
  val sc = new SparkContext("local","WordsCountInvertIndex")
  val pathSeparator = File.separator

    val fileContent = sc.wholeTextFiles(shakespeare.toString).
      flatMap {
        case (location, contents) =>
          val words = contents.split("""\W+""").
            filter(word => word.size > 0) // #1
          val fileName = location.split("\\\\").last
          words.map(word => ((word.toLowerCase, fileName), 1)) // #2
      }.
      reduceByKey((count1, count2) => count1 + count2).
      map {
        case ((word, fileName), count) => (word, (fileName, count))
      }.
      groupByKey.
      sortByKey(ascending = true).
      mapValues { iterable =>
        val vect = iterable.toVector.sortBy {
          case (fileName, count) => (-count, fileName)
        }
        vect.mkString(",")
      }

  val sqlContext = new SQLContext(sc)
    
  //val dataFrame = sqlContext.createDataFrame(fileContent).toDF("word", "location_counts")
  
  fileContent.foreach(println)
  
  }
}