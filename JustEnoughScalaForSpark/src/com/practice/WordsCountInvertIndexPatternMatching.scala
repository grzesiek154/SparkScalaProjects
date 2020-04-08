package com.practice
import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordsCountInvertIndexPatternMatching {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val shakespeare = new File("../data/shakespeare")
    val sc = new SparkContext("local", "WordsCountInvertIndex")
    val pathSeparator = File.separator
    val ii1 = sc.wholeTextFiles(shakespeare.toString).
      flatMap {
        case (location, contents) =>
          val words = contents.split("""\W+""").
            filter(word => word.size > 0) // #1
          val fileName = location.split(pathSeparator).last
          words.map(word => ((word.toLowerCase, fileName), 1)) // #2
//          The case keyword says I want to pattern match on the object passed to the function. If it's a
//two-element tuple (and I know it always will be in this case), then extract the first element and assign it to a
//variable named location and extract the second element and assign it to a variable named contents .
      }.
      reduceByKey((count1, count2) => count1 + count2).
//      to be clear, this isn't a pattern-matching expression; there is no case keyword. It's just a "regular" function that
//takes two arguments, for the two things I'm adding.
      map {
        case ((word, fileName), count) => (word, (fileName, count))
//        The new implementation makes it clear what I'm doing; just shifting parentheses! That's all it takes to go from the
//(word, fileName) keys with count values to word keys and (fileName, count) values. Note that
//pattern matching works just fine with nested structures, like ((word, fileName), count) .
      }.
      groupByKey.
      sortByKey(ascending = true).
      mapValues { iterable =>
        val vect = iterable.toVector.sortBy {
          case (fileName, count) => (-count, fileName)
//          The function I pass to sortBy returns a tuple used for sorting, with -count to force descending numerical
//sort (biggest first) and fileName to secondarily sort by the file name, for equivalent counts. I could ignore file
//name order and just return -count (not a tuple). However, if you need more repeatable output in a distributed
//system like Spark, say for example to use in unit test validation, then the secondary sorting by file name is
//handy
        }
        vect.mkString(",")
      }
  }
}