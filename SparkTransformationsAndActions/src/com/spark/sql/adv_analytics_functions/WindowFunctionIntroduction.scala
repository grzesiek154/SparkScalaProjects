package com.spark.sql.adv_analytics_functions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

//Window functions can be categorized into three different types: ranking functions,
//analytic functions, and aggregate functions. 

// Frame - logical grouping of rows

object WindowFunctionIntroduction extends App{

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
    
    // 1. For each user, what are the two highest transaction amounts?

  // define window specification to partition by name and order by amount in descending amount
  val forRankingWindow = Window.partitionBy("name").orderBy(desc("amount"))
  
  // add a new column to contain the rank of each row, apply the rank function to rank each row
  val txDataWithRankDF = txDataDF.withColumn("rank", rank().over(forRankingWindow))
  
  txDataWithRankDF.where('rank < 3).show()
  
  //2.What is the difference between the transaction amount of each user and their highest transaction amount?
   
  // use rangeBetween to define the frame boundary that includes all the rows in each frame
  val forEntireRangeWindow = Window.partitionBy("name")
                                    .orderBy(desc("amount"))
                                    .rangeBetween(Window.unboundedPreceding,
                                     Window.unboundedFollowing)  
  // rangeBetweend measn that we create window of rows with particular limitations in above example 
  // window is created from first val in the row to the last                                    
  
  // apply the max function over the amount column and then compute the difference
  val amountDifference = max(txDataDF("amount")).over(forEntireRangeWindow) - txDataDF("amount")
                                 
  
  // add the amount_diff column using the logic defined above
  val txDiffWithHighestDF = txDataDF.withColumn("amount_diff", round(amountDifference, 3))
  
  txDiffWithHighestDF.show
  
  //3.What is the moving average transaction amount of each user?
  
 // define the window specification
// a good practice is to specify the offset relative to Window.currentRow
  val forMovingAvgWindow = Window.partitionBy("name").orderBy("tx_date")
                            .rowsBetween(Window.currentRow -1, Window.currentRow + 1)
//   For this particular example, you want each
//frame to include three rows: the current row plus one row before it and one row after it.
//Depending a particular use case, the frame might include more rows before and after the
//current row.                          
                            
  // apply the average function over the amount column over the window specification
  // also round the moving average amount to 2 decimals
   val txMovingAvgDF = txDataDF.withColumn("moving_avg",round(avg("amount").
                                                               over(forMovingAvgWindow), 2))
   txMovingAvgDF.show()                                                            
  
  //4.What is the cumulative sum of the transaction amount of each user?
   
   // define the window specification with each frame includes all the previous rows and the current row
   
   val forCumulativeSumWindow = Window.partitionBy("name").orderBy("tx_date")
                                                     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                                                     
   // apply the sum function over the window specification 
   val txCumulativeSumDF = txDataDF.withColumn("culm_sum", round(sum("amount")
                                             .over(forCumulativeSumWindow), 2))
   txCumulativeSumDF.show()                                          
}