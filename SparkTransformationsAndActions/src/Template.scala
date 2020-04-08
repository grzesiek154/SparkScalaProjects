import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Template extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleQueries")
      .getOrCreate()
      
   import spark.implicits._
  
}