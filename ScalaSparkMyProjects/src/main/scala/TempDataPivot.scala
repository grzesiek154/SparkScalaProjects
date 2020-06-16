import java.util.logging.Level

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object TempDataPivot extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()
  import spark.implicits._

  val tempDataSchema = StructType(Array(
    StructField("day", IntegerType, true),
    StructField("id", IntegerType, true),
    StructField("month", IntegerType, true),
    StructField("state_id", StringType, true),
    StructField("year", IntegerType, true),
    StructField("prcp(in)", DecimalType(10,2), true),
    StructField("snow(in)", DecimalType(10,2), true),
    StructField("tave(in)", DecimalType(10,2), true),
    StructField("tmax(in)", DecimalType(10,1), true),
    StructField("tmin(in)", DecimalType(10,2), true)

  ))

  val tempDataDF = spark.read.schema(tempDataSchema).option("header", true).csv("../data/MN212142_9392.csv")
  val tempDataDF2 = spark.read.option("header", false).csv("../data/MN212142_9392.csv")

  val testMap = Map(1 -> "asd", 2 -> "zxc", 3 -> "qwe")

  val tempPivot =  tempDataDF.groupBy('day).pivot('month).avg("tmax(in)").orderBy('day.asc)

  //tempPivot.filter("day is not null").show(10)


  val testDateTSDF = Seq((1, "2018-01-01", "2018-01-01 15:04:58:865", "01-01-2018", "12-05-2017 45:50")).toDF("id", "date", "timestamp","date_str", "ts_str")

  testDateTSDF.printSchema()

  // convert these strings into date, timestamp and unix timestamp
  // and specify a custom date and timestamp format
  val testDateResultDF = testDateTSDF.select(to_date('date).as("date1"),to_timestamp('timestamp).as("ts1"),to_date('date_str,"MM-dd-yyyy").as("date2"),
    to_timestamp('ts_str,"MM-dd-yyyy mm:ss").as("ts2"),unix_timestamp('timestamp).as("unix_ts")).show(false)






}
