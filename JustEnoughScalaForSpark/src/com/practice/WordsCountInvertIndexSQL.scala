package com.practice
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.SQLContext

object WordsCountInvertIndexSQL {
  
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    
    val shakespeare = new File("../data/shakespeare")
    val sc = new SparkContext("local", "WordsCountInvertIndex")
    val sqlContext = new SQLContext(sc)
//    SQLContext is a class and is used for initializing the functionalities of Spark SQL.
//    SparkContext class object (sc) is required for initializing SQLContext class object.
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
      map { 
        
        case (word, iterable) =>
        val vect = iterable.toVector.sortBy {
          case (fileName, count) => (-count, fileName)

        }
        val (locations, counts) = vect.unzip
        // Use `Vector.unzip`, which returns a single, two element tuple, where each
        // element is a collection, one for the locations and one for the counts.
        // I use pattern matching to extract these two collections into variables.
        
        
        val totalCount = counts.reduceLeft((n1,n2) => n1+n2)
            (word, totalCount, locations, counts)
      }
      
      fileContent.foreach(println)
      
      
//      val fileContendDF = sqlContext.createDataFrame(fileContent).toDF("word", "location_and_count")
//      fileContendDF.cache()
//      //fileContendDF.createOrReplaceTempView("inverted_index")
//      fileContendDF.registerTempTable("inverted_index")
    
      
//      Caching the DataFrame in memory prevents Spark from recomputing ii from the input files every time I
//      write a query!
//      
//      fileContendDF.printSchema()
//      val topLocations = sqlContext.sql("""
//        SELECT word, location_and_count[0] AS top_location, location_and_count[1] AS top_count 
//        FROM inverted_index
//        WHERE word LIKE '%love%' OR word LIKE '%hate%'
//        """)
//        topLocations.show()
  }
}