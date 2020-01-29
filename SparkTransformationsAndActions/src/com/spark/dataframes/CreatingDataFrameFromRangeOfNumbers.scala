package com.spark.dataframes
import org.apache.log4j._
import org.apache.spark.sql.SparkSession



object CreatingDataFrameFromRangeOfNumbers {
  
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameFromRangeOfNumbers")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      import spark.implicits._
      
     // implicits object gives implicit conversions for converting Scala objects (incl. RDDs) into a Dataset, DataFrame, Columns or supporting such conversions (through Encoders).
//      Scala “implicits” allow you to omit calling methods or referencing variables directly but instead rely on the compiler to make the connections for you. 
//      For example, you could write a function to convert from and Int to a String and rather than call that function explicitly, 
//      you can ask the compiler to do it for you, implicitly.
      
      val df1 = spark.range(5).toDF("num").show()
      val df2 = spark.range(5,10).toDF("num").show
      val df3 = spark.range(5,15,2).toDF("num").show

      
//      The previous version of the range function takes three parameters. The first one
//represents the starting value, the second one represents the end value (exclusive), and
//the last one represents the step size. Notice the range function can create only a singlecolumn DataFrame.
      
      
      //Converting a Collection Tuple to a DataFrame Using Spark’s toDF Implicit
      val movies = Seq(("Damon, Matt", "The Bourne Ultimatum", 2007L), ("Damon, Matt", "Good Will Hunting", 1997L))
      val moviesDF = movies.toDF("actor","title", "year")
      
      moviesDF.printSchema
      moviesDF.show
  }
}