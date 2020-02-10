package com.spark.rdd.transformations

import org.apache.spark.SparkContext

object CaseClassTest {

  case class IIRecord(
    word:        String,
    total_count: Int = 0,
    locations:   Array[String] = Array.empty,
    counts:      Array[Int]    = Array.empty) {

    override def toString: String =
      s"""IIRecord($word, $total_count, $locStr, $cntStr)""" // s is a string interpolation

    def toCSV: String = s"$word,$total_count,$locStr,$cntStr"

    /** Return a JSON-formatted string for the instance. */
    def toJSONString: String =
      s"""{
          | "word": "$word",
          | "total_count": $total_count,
          | "locations": ${toJSONArrayString(locations)},
          | "counts" ${toArrayString(counts, ", ")}
          |}
          |""".stripMargin

    private def locStr = toArrayString(locations)

    private def cntStr = toArrayString(counts)
    
    // "[_]" means we don't care what type of elements; we're just
    // calling toString on them!
    private def toArrayString(array: Array[_], delim: String = ","): String =
      array.mkString("[", delim, "]") // i.e., "[a,b,c]"

    private def toJSONArrayString(array: Array[String]): String =
      toArrayString(array.map(quote), ", ")

    private def quote(word: String): String = "\"" + word + "\""
  }

  def main(args: Array[String]) {

    val hello = IIRecord("hello")
    
    println(hello)
    
    println(hello.toJSONString)
    
    println(hello.toCSV)
  }

}