package com.streaming.DStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel

object NetworkWordCount extends App {

  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
//  There are a few important steps when putting together a DStream application. The
//entry point to a DStream application is StreamingContext, and one of the required
//inputs is a batch interval, which defines a time duration that Spark uses to batch
//incoming data into an RDD for processing.
  
 //It also represents a trigger point for when
//Spark should execute the streaming application computation logic. For example,
//if the batch interval is three seconds, then Spark batches all the data that arrives
//within that three-second interval; after that interval elapses, it will turn that batch of
//data into an RDD and process it according to the processing logic you provide. 
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val host = "localhost"
  val port = 9999

  val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK)
  val words = lines.flatMap(_.split(" "))
  val wordsCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  wordsCounts.print()
  ssc.start()
//  Remember that a streaming application is
//a long-running application; therefore, it requires a signal to start the task of receiving
//and processing the incoming stream of data. That signal is given by calling the
//start() function of StreamingContext, and this is usually done at the end of the file
  ssc.awaitTermination()
//  The awaitTermination() function is used to wait for the execution of the streaming
//application to stop as well as a mechanism to prevent the driver from exiting while your
//streaming application is running. In a typically program, once the last line of code is
//executed, it will exit. However, a long-running streaming application needs to keep going
//once it is started and will end only when you explicitly stop it
}