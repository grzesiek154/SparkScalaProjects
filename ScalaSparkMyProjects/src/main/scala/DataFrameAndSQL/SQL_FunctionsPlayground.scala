package DataFrameAndSQL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object SQL_FunctionsPlayground extends App{



  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()
  import spark.implicits._

  val vixDataSchema = StructType(Array(
    StructField("date", StringType, true),
    StructField("vix_open", DoubleType, true),
    StructField("vix_high", DoubleType, true),
    StructField("vix_low", DoubleType, true),
    StructField("vix_close", DoubleType, true)
  ))

  val vixDataTemp = spark.read.schema(vixDataSchema).csv("../data/vix-daily.csv").toDF()
  val headers = vixDataTemp.first()
  val vixDataDF = vixDataTemp.filter(line => line != headers)



  //vixDataDF.show(10)
  vixDataDF.select('date,
    when(dayofweek('date) === 1, "Mon")
      .when(dayofweek('date) === 2, "Tue")
      .when(dayofweek('date) === 3, "Wed")
      .when(dayofweek('date) === 4, "Thu")
      .when(dayofweek('date) === 5, "Fri")
      .when(dayofweek('date) === 6, "Sat")
      .when(dayofweek('date) === 7, "Sun")
  .as("dayOfTheWeek")).show(20)


}
