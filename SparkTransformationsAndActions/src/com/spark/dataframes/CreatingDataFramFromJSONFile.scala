package com.spark.dataframes
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.IntegerType

object CreatingDataFramFromJSONFile {

  //  Spark makes it easy to read data in a JSON file. However, there is one thing that
  //you need to pay attention to. A JSON object can be expressed on a single line or across
  //multiple lines, and this is something you need to let Spark know. Given that a JSON data
  //file contains only column names and no data type, how is Spark able to come up with a
  //schema? Spark tries its best to infer the schema by parsing a set of sample records. The
  //number of records to sample is determined by the samplingRatio option, which has a
  //default value of 1.0. Therefore, it is quite expensive to read a large JSON file. In this case,
  //you can lower the samplingRatio value to speed up the data loading process.

  //  Table 4-5. JSON Common Options
  //Key Values Default Description
  //allowComments true, false false ignores comments in the JSon file
  //multiLine true, false false treats the entire file as a large JSon object that spans
  //many lines
  //samplingRatio 0.3 1.0 Specifies the sampling size to read to infer the schema

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    val movies = spark.read.json("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.json")
    // Notice Spark automatically detects the column name and data type based on the information in the JSON file. The second example specifies a schema

    val movieShema = StructType(Array(
      StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", StringType, true)))
      
    val badMovieSchema = StructType(Array(
      StructField(
      "actor_name",
      BooleanType, true),
      StructField(
        "movie_title",
        StringType, true),
      StructField(
        "produced_year",
        IntegerType, true)))

    val movies2 = spark.read.option("inferShema", "true").schema(movieShema).json("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.json")
    val movies3 = spark.read.option("inferShema", "true").schema(badMovieSchema).json("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.json")

    //       What happens when a column data type specified in the schema doesn’t match up
    //      with the value in the JSON file? By default, when Spark encounters a corrupted record or
    //      runs into a parsing error, it will set the value of all the columns in that row to null. Instead
    //      of getting null values, you can tell Spark to fail fast
    
    //tell spark to failFast
     val movies4 = spark.read.option("mode", "failFast").schema(badMovieSchema).json("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.json")

    movies4.printSchema()
    
    movies4.show(5)
  }
}