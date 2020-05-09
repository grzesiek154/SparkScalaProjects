import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Template extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()
      
   import spark.implicits._
   
 
  
}