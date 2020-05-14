package com.spark.sql.functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//The Spark built-in date-time functions broadly fall into the following three categories: converting
//the date or timestamp from one format to another, performing date-time calculations,
//and extracting specific values from a date or timestamp.

object DateAndTimeFunctions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  //1. Converting a date and timestamp in string type to Spark Date and Timestamp type

  val testDateTSDF = Seq((1, "2018-01-01", "2018-01-01 15:04:58:865", "01-01-2018", "12-05-2017 45:50")).toDF("id", "date", "timestamp", "date_str", "ts_str")

  // convert these strings into date, timestamp and unix timestamp
  // and specify a custom date and timestamp format
  val testDateResultDF = testDateTSDF.select(to_date('date).as("date1"), to_timestamp('timestamp, "yyyy-dd-MM mm:ss").as("ts1"), to_date('date_str, "MM-dd-yyyy").as("date2"),
    to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"), unix_timestamp('timestamp).as("unix_ts")).toDF()
  // date1 and ts1 are of type date and timestamp respectively

  testDateResultDF.printSchema()
  testDateResultDF.show

  //2. Converting a Date, Timestamp, and Unix Timestamp to a String

  testDateResultDF.select(date_format('date1, "dd-MM-YYYY").as("date_str"), date_format('ts1, "dd-MM-YYYYHH:mm:ss").as("ts_str"),
    from_unixtime('unix_ts, "dd-MM-YYYYHH:mm:ss").as("unix_ts_str")).show

  //3. Date-Time Calculation Examples
  val employeeData = Seq(("John", "2016-01-01", "2017-10-15"), ("May", "2017-02-06", "2017-12-25")).toDF("name", "join_date", "leave_date")
  employeeData.select('name, datediff('leave_date, 'join_date).as("days"),
    months_between('leave_date, 'join_date).as("months"),
    last_day('leave_date).as("last_day_of_month")).show

  val oneDate = Seq(("2018-01-01")).toDF("new_year")
  oneDate.select(
    date_add('new_year, 14).as("mid_month"),
    date_sub('new_year, 1).as("new_year_eve"),
    next_day('new_year, "Mon").as("next_mon")).show

  //4. Extracting Specific Fields from a Date Value

  val valentimeDateDF = Seq(("2018-02-14 05:35:55")).toDF("date")
  valentimeDateDF.select(
    year('date).as("year"),
    quarter('date).as("quarter"),
    month('date).as("month"),
    weekofyear('date).as("woy"),
    dayofmonth('date).as("dom"),
    dayofyear('date).as("doy"),
    hour('date).as("hour"),
    minute('date).as("minute"),
    second('date).as("second"))
    .show
}
