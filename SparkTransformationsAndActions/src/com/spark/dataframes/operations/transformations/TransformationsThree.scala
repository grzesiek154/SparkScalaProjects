package com.spark.dataframes.operations.transformations
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object TransformationsThree {
  
    def main(args: Array[String]) {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
      import spark.implicits._
      
//            limit(n)
//      This transformation returns a new DataFrame by taking the first n rows. This
//      transformation is commonly used after the sorting is done to figure out the top n or
//      bottom n rows based on the sorting order
      
      
       val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
       
       val actorName = movies.select("actor_name").distinct.selectExpr("*", "length(actor_name) as length")
       
       actorName.orderBy('length.desc).limit(10).show

//       union(otherDataFrame)
//You learned earlier that DataFrames are immutable. So if there is a need to add more
//rows to an existing DataFrame, then the union transformation is useful for that purpose
//as well as for combining rows from two DataFrames. This transformation requires
//both DataFrames to have the same schema, meaning both column names and their
//order must exactly match. Let say one of the movies in the DataFrame is missing an
//actor and you want to fix that issue.
       
       val shortNameMovieDF = movies.where('movie_title === "12")
       shortNameMovieDF.show()
       
       val forgottenActor = Seq(Row("Zenon, Martyniuk", "12", 2007L))
       val forgottenActorRDD = spark.sparkContext.parallelize(forgottenActor)
       val forgottenActorDF = spark.createDataFrame(forgottenActorRDD, shortNameMovieDF.schema)
       
       val shortNameMovieDFComplete = shortNameMovieDF.union(forgottenActorDF).show()
      
       
//       withColumn(colName, column)
//This transformation is used to add a new column to a DataFrame. It requires two input
//parameters: a column name and a value in the form of a column expression. You
//can accomplish pretty much the same goal by using the selectExpr transformation.
//However, if the given column name matches one of the existing ones, then that column
//is replaced with the given column expression. 
       
      // adding a new column based on a certain column expression
      movies.withColumn("decade", ('produced_year - 'produced_year % 10)).show(5) 
      // adding a new column based on a certain column expression
      movies.withColumn("decade", ('produced_year - 'produced_year % 10)).show(5) 
      
//      withColumnRenamed(existingColName, newColName)
//This transformation is strictly about renaming an existing column name in a DataFrame.
//It is fair to ask why in the world Spark provides this transformation. As it is turns out, this
//transformation is useful in the following situations:
//• To rename a cryptic column name to a more human-friendly name.
//The cryptic column name can come from an existing schema that
//you don’t have control of, such as when the column you need in a
//Parquet file was produced by your company’s partner.
//• Before joining two DataFrames that happen to have one or more
//same column name. This transformation can be used to rename one
//or more columns in one of the two DataFrames so you can refer to
//them easily after the join.
//Notice that if the provided existingColName doesn’t exist in the schema, Spark
//doesn’t throw an error, and it will silently do nothing. Listing 4-32 renames some of the
//column names in the movies DataFrame to short names. By the way, this is something
//that can be accomplished by using the select or selectExpr transformation. I will leave
//that as an exercise for you.
      
      movies.withColumnRenamed("actor_name", "actor")
            .withColumnRenamed("movie_title", "title")
            .withColumnRenamed("produced_year", "year").show(5)
            
            
//            drop(columnName1, columnName2)
//This transformation simply drops the specified columns from the DataFrame. You can
//specify one or more column names to drop, but only the ones that exist in the schema
//will be dropped and the ones that don’t will be silently ignored. You can use the select
//transformation to drop columns by projecting only the columns that you want to keep.
//In the case that a DataFrame has 100 columns and you want to drop a few, then this
//transformation is more convenient to use than the select transformation
            movies.drop("actor_name", "me").printSchema
            
            
//            sample(fraction), sample(fraction, seed), sample(fraction, seed,
//withReplacement)
//This transformation returns a randomly selected set of rows from the DataFrame. The
//number of the returned rows will be approximately equal to the specified fraction, which
//represents a percentage, and the value has to be between 0 and 1. The seed is used to
//seed the random number generator, which is used to generate a row number to include
//in the result. If a seed is not specified, then a randomly generated value is used. The
//withReplacement option is used to determine whether a randomly selected row will be
//placed back into the selection pool. In other words, when withReplacement is true, a
//particular selected row has the potential to be selected more than once. So, when would
//you need to use this transformation? It is useful in the case where the original dataset is
//large and there is a need to reduce it to a smaller size so you can quickly iterate on the
//data analysis logic. 
            
            // sample with no replacement and a fraction
            movies.sample(false, 0.0003).show(3)
            
            // sample with no replacement and a fraction
            movies.sample(false, 0.0003).show(3)
            
            
//            randomSplit(weights)
//This transformation is commonly used during the process of preparing the data to train
//machine learning models. Unlike the previous transformations, this one returns one
//or more DataFrames. The number of DataFrames it returns is based on the number of
//weights you specify. If the provided set of weights don’t add up to 1, then they will be
//normalized accordingly to add up to 1
            
            // the weights need to be an Array
            val smallerMovieDFs = movies.randomSplit(Array(0.6, 0.3, 0.1))
            
            // let's see if the counts are added up to the count of movies DataFrame
            movies.count      
            //Long = 31393
            smallerMovieDFs(0).count
            //Long = 18881
            smallerMovieDFs(0).count + smallerMovieDFs(1).count + smallerMovieDFs(2).count
            //Long = 31393
            
//    describe(columnNames)
//Sometimes it is useful to have a general sense of the basic statistics of the data you
//are working with. The basic statistics this transformation can compute for string and
//numeric columns are count, mean, standard deviation, minimum, and maximum.
//You can pick and choose which string or numeric columns to compute the statistics for
            movies.describe("produced_year").show
  }
}