package data_filtering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

case class SuperHero (id:Long, name:String)
case class HeroCoOccurrences (id:Long, others:Seq[Long])


object JoinsAndGroupingPlayground  extends App  {

  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()

  def countCoOccurrences(line: String) = {
    var elements = line.split("\\s+")
    HeroCoOccurrences( elements(0).toInt, Seq(elements.length - 1 ))
  }

  def getHeroIdAndName(line:String): Option[SuperHero] = {
    val elements = line.split('\"')
    if(elements.length > 1) {
      Some(SuperHero(elements(0).trim.toInt, elements(1)))
    } else {
      None
    }
  }

  val movesAndActorsSchema = StructType(Array(StructField("actor", StringType, true),
    StructField("title", StringType, true),
    StructField("produced_year", StringType, true)))


  val marvelData = spark.sparkContext.textFile("../data/Marvel-names.txt").flatMap(line => getHeroIdAndName(line))
  val heroCoOccurrences = spark.sparkContext.textFile("../data/Marvel-graph.txt").map(line => countCoOccurrences(line))

  val marvelDataDF = spark.createDataFrame(marvelData)
  val heroCoOccurrencesDF = spark.createDataFrame(heroCoOccurrences)

  val temData = spark.read.csv("../data/1800.csv")

  // joining two columns with marvel data and removin duplicated id column
  val heroJoinData = marvelDataDF.join(heroCoOccurrencesDF, marvelDataDF.col("id") === heroCoOccurrencesDF.col("id"))
                                                                                            .drop(heroCoOccurrencesDF.col("id"))
  val heroJoinData2 = marvelDataDF.join(heroCoOccurrencesDF, marvelDataDF.col("id") === heroCoOccurrencesDF.col("id"),"outer")


  // working wit vix data

  val vixDataSchema = StructType(Array(
    StructField("date", StringType, true),
    StructField("vix_open", DoubleType, true),
    StructField("vix_high", DoubleType, true),
    StructField("vix_low", DoubleType, true),
    StructField("vix_close", DoubleType, true)
  ))

  case class VixDailyData (date:String, vix_open:Double, vix_high:Double, vix_low:Double, vix_close:Double)

  val customerOrdersDF = spark.read.schema(vixDataSchema).csv("../data/vix-daily.csv")
  val headers = customerOrdersDF.first()
  val customerOrdersDF2 = customerOrdersDF.filter(line => line != headers)


  temData.show(10)








}
// to do
// 1. Przefiltrowac dne aby pozybc sie naglowkow
// 2. Dac danym schemat clasy
// 3. Pobawic sie z pivot i agregacjami