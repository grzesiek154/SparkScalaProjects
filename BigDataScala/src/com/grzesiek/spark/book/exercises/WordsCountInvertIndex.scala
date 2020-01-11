package com.grzesiek.spark.book.exercises
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._


object WordsCountInvertIndex {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val shakespeare = new File("../data/shakespeare")
  val sc = new SparkContext("local","WordsCountInvertIndex")
  val pathSeparator = File.separator
  
  val fileContent = sc.wholeTextFiles(shakespeare.toString). // fileContent.keys - sciezki do plikow w folderze, fileContentValues - zawartosci plikow z folderu
  
      flatMap { location_contents_tuple2 =>
        val words = location_contents_tuple2._2.split("""\W+""")
        val fileName = location_contents_tuple2._1.split("\\\\").last
        words.map(word => ((word, fileName), 1))
       
        
//        Wait, I said we're passing a function as an argument to flatMap . If so, why am I using braces {...} around
//this function argument instead of parentheses (...) like you would normally expect when passing arguments
//to a method like flatMap ?
//It's because Scala lets us substitute braces instead of parentheses so we have the familiar block-like syntax
//{...} we know and love for if and for expressions. I could use either braces or parentheses here. The
//convention in the Scala community is to use braces for a multi-line anonymous function and to use parentheses
//for a single expression when it fits on the same line.
    
  }.
  reduceByKey((total, value) => total + value). // The last expression in the block, message, is the return value.
                                                //Dlatego mimo ze tuple na ktorym dzialamy posiada 3 wartosci, reduceByKey bedzie wykonywac obliczenia dla ostatnie wartosci
  map { word_file_count_tup3 => (word_file_count_tup3._1._1, (word_file_count_tup3._1._2, word_file_count_tup3._2))
  
  
//    Note that the anonymous function reduceByKey expects must take two arguments, so I need parentheses
//around the argument list. Since this function fits on the same line, I used parentheses for reduceByKey ,
//instead of braces.
//Note: All the *ByKey methods operate on two-element tuples and treat the first element as the
//key, by default.
//  Notes:
//For historical reasons, tuple indices start at 1, not 0. Arrays and other Scala collections index
//from 0.
//I said previously that method arguments have to be declared with types. That's usually not
//required for function arguments, as here.
//Another benefit of triple-quoted strings that makes them nice for regular expressions is that
//you don't have to escape regular expression metacharacters, like \W . If I used a singlequoted
//string, I would have to write it as "\\W+" . Your choice...
//Let's
    
  }.
  groupByKey.
  sortByKey(ascending = true).
  mapValues {iterable =>
    val vect = iterable.toVector.sortBy { file_count_tup2 =>
     (-file_count_tup2._2, file_count_tup2._1)
      
    }
    vect.mkString(",")
  }  
      fileContent.foreach(println)
  
   fileContent.foreach(println)
    
  }
  

}