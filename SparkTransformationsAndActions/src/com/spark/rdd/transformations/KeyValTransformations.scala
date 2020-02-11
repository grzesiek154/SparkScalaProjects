package com.spark.rdd.transformations

import org.apache.log4j._;
import org.apache.spark.SparkContext;

object T_And_A_Playground {
  
  
  
 
  
  def main(args: Array[String]) {
 
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    //Creating Key/Value Pair RDD
    val sc = new SparkContext("local[*]", "T_And_A_Playground")
    
    val rdd = sc.parallelize(List("Spark","is","an", "amazing", "piece","of","technology"))
    val pairRdd = rdd.map(w => (w.length, w))
    pairRdd.collect().foreach(println)
    
    
    
    //groupByKey([numTasks])
      println("!!!!!!!!!!!groupByKey():")
      val wordByLenRdd = pairRdd.groupByKey()
      wordByLenRdd.collect().foreach(println)     
      
// reduceByKey()
 //      This transformation is often used to reduce all the values of the same key to a single
//value. The process is carried out in two steps, as depicted in Figure 3-1. The first one
//is to group the values of the same key together, and the second step is to apply the
//given reduce function to the list of values of each key. The implementation of this
//transformation contains a built-in optimization to perform this two-step process at two
//levels. The first level is at each individual partition, and the second level is across all the
//partitions. By applying this transformation at each individual partition first, it therefore
//collapses all the rows with the same key in the same partition to a single row, and as a
//result, the amount of data that needs to be moved across many partitions is dramatically
//reduced.
      
      val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
                                                          ("candy1", 2.0),
                                                          ("candy2", 6.0),
                                                          ("candy3", 3.0)))
                                                          
      val summaryTx = candyTx.reduceByKey((total, value) => total + value)
      println("!!!!!!!!!!!reduceByKey():")
      summaryTx.collect().foreach(println)
      
      
//      sortByKey([ascending],[numTasks])
//This transformation is simple to understand. It sorts the rows according the key,
//and there is an option to specify whether the result should be in ascending (default)
      
      val summaryByPrice = summaryTx.map(t => (t._2, t._1)).sortByKey()
      summaryByPrice.collect().foreach(println)
      
      val summaryByPrice2 = summaryTx.map(t => (t._2, t._1)).sortByKey(false)
      summaryByPrice2.collect().foreach(println)
      
      
      
//      join(otherRDD)
//Performing any meaningful data analysis usually involves joining two or more datasets.
//The join transformation is used to combine the information of two datasets to enable
//rich data analysis or to extract insights. For example, if one dataset contains the
//transaction information and it has a member ID and details of the transaction and
//another dataset contains the member information, by joining these two datasets you can
//answer questions such as, what is the breakdown of the transactions by the age group,
//and which age group made the largest number of transactions?
//By joining the dataset of type (K,V) and dataset (K,W), the result of the joined
//dataset is (K,(V,W)). There are several variations of joining two datasets, like left and
//right outer joins
      
      val memberTx = sc.parallelize(List((110, 50.35), (127, 305.2), (126, 211.0),
                    (105, 6.0),(165, 31.0), (110, 40.11)))
      val memberInfo = sc.parallelize(List((110, "a"), (127, "b"), (126, "b"),
                      (105, "a"),(165, "c")))
                      
       val joinTxInfo = memberTx.join(memberInfo)
       println("!!!!Join starts!!!!!")
       joinTxInfo.collect().foreach(println)
      
      
  }
}