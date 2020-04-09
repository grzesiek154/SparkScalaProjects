package data_filtering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RegexTest {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def filterTextFile(line: String) = {

    val pattern = "^the".r
    pattern.findFirstIn(line)
  }

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataFramesTransformations")
      .getOrCreate()
    import spark.implicits._

    val rddTextFile = spark.sparkContext.textFile("book.txt").flatMap(line => line.split("\\n"))



    //val book = scala.io.Source.fromFile("book.txt").toString()

//    println(rddTextFile)
    val pattern = rddTextFile.take(1)
    //rddTextFile.foreach(println)

    pattern.foreach(println)

  }
}
