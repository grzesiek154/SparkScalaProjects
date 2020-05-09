package data_filtering

import com.sun.xml.internal.bind.v2.TODO
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex


//scala.util.matching.Regex.MatchIterator = non-empty iterator - means that returned value is an iterator and need to be accessed via proper method


object RegexTest {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val address = "123 Main Street Suite 101"
  val someList = List(1,2,44,5,76,234,9)

  val numPattern = "[0-9]+".r
  val textPattern = "Will".r

  def filterData(line: String, rgx:Regex) = {

    rgx.findFirstIn(line)
  }

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataFramesTransformations")
      .getOrCreate()
    import spark.implicits._

    val someText = "to jest testowy teks 123 asd 987"
    val textRDD = spark.sparkContext.parallelize(someText)
    val rddTextFile = spark.sparkContext.textFile("book.txt").flatMap(line => line.split("\\n"))

    val fakeFriends = scala.io.Source.fromFile("fakefriends.csv").getLines()

    val test = fakeFriends.flatMap(line => filterData(line, textPattern))
    val pattern = rddTextFile.take(1)


    rddTextFile.foreach(println)

    //TODO
    // 1. MATCHED VALUES FORM CSV TO A LIST
    // 2. PRACTICE OTHER REGEX AND MAKE A DUCMENTATION
    // 3. PREPARE REGEX CHET SHEET
  }
}
