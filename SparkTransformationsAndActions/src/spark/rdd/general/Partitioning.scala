package spark.rdd.general
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Partitioning extends App {
  //      Spark can run 1 concurrent task for every partition of an RDD (up to the number of cores in the cluster). If you’re cluster has 20 cores, you should have at least 20 partitions (in practice 2–3x times more). From the other hand a single partition typically shouldn’t contain more than 128MB and a single shuffle block cannot be larger than 2GB (see SPARK-6235).
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]") // if we use 2 core data will be splite between two partitions
    .appName("MultipleQueries")
    .getOrCreate()

  val numRange = 1 to 10

//  val rdd = spark.sparkContext.parallelize(numRange, 15)
  // if we provide more parititions than we have elements there will be empty partitions
  
  val rdd = spark.sparkContext.parallelize(numRange).map(x=> (x,x))
  val someList = rdd.glom().collect()
 


  //    glom()- return an RDD created by coalescing all elements within each partition into a list.
  //    partitioner - inspect partitioner information used for the RDD.

  println("Default Pararelism: " + spark.sparkContext.defaultParallelism)
  println("num of partitions: " + rdd.getNumPartitions)
  println("partitioner: " + rdd.partitioner)
  someList.foreach(x => x.mkString(", ").foreach(print))

  //    partitionBy() transformation allows applying custom partitioning logic over the RDD.
  //        Warning — to use partitionBy() RDD must consist of tuple (pair) objects. It's a transformation, so a new RDD will be returned. It's highly adviseable to persist it for more optimal later usage.
  //
  //Because partitionBy() requires data to be in key/value format we will need to transform the data.
  
  
  
}