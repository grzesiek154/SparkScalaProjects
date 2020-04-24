package com.spark.sql.adv_analytics_functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctionsSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("MultipleQueries")
    .getOrCreate()

  import spark.implicits._

  val txDataDF = Seq(
    ("John", "2017-07-02", 13.35),
    ("John", "2017-07-06", 27.33),
    ("John", "2017-07-04", 21.72),
    ("Mary", "2017-07-07", 69.74),
    ("Mary", "2017-07-01", 59.44),
    ("Mary", "2017-07-05", 80.14))
    .toDF("name", "tx_date", "amount")

  // register the txDataDF as a temporary view called tx_data
  txDataDF.createOrReplaceTempView("tx_data")
  // use RANK window function to find top two highest transaction amount
  spark.sql("select name, tx_date, amount, rank from(select name, tx_date, amount, RANK() OVER (PARTITION BY name ORDER BY amount DESC) as rank from tx_data) where rank < 3").show

  // difference between maximum transaction amount
  spark.sql(
    "select name, tx_date, amount, round((max_amount - amount),2) as amount_diff from (select name, tx_date, amount, MAX(amount) OVER(PARTITION BY name ORDER BY amount DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as max_amount from tx_data)").show

  // moving average
  spark.sql(
    "select name, tx_date, amount, round(moving_avg,2) as moving_avg from ( select name, tx_date, amount, AVG(amount) OVER(PARTITION BY name ORDER BY tx_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as moving_avg from tx_data)").show

  // cumulative sum
  spark.sql(
    "select name, tx_date, amount, round(culm_sum,2) as moving_avg FROM ( select name, tx_date, amount, SUM(amount) OVER PARTITION BY name ORDER BY tx_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) as culm_sum from tx_data)").show
}