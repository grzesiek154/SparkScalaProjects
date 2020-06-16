package spark.rdd.general
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object PartitioningDF extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[8]") // if we use 2 core data will be splite between two partitions
    .getOrCreate()

  val movies = spark.read.json("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.json")
  
  movies.printSchema()
  
//  The rules for using are as follows:
//
//    if you are increasing the number of partitions use repartition()(performing full shuffle),
//    if you are decreasing the number of partitions use coalesce() (minimizes shuffles)
  
  val df = spark.range(0,20)
  val movies2 = movies.repartition(4)
  val movies3 = movies.coalesce(2)
  
  println("range df: " + df.rdd.getNumPartitions)
  println("before repartition: " + movies.rdd.getNumPartitions)
  println("after repartition: " + movies2.rdd.getNumPartitions)
  println("after colacase: " + movies3.rdd.getNumPartitions)

}