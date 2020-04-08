import org.apache.spark.sql.SparkSession
import org.apache.log4j._

case class Movie(actor_name:String, movie_title:String, produced_year:Option[Long])

object DataFramesTransformations extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()
    import spark.implicits._

    val movies = spark.read.parquet("../data/beginning-apache-spark-2-master/chapter4/data/movies/movies.parquet")
    movies.createOrReplaceTempView("movies")
    val moviesSQL = spark.sql("SELECT actor_name, COUNT(movie_title) as actor_movies FROM movies GROUP BY actor_name ORDER BY actor_movies DESC")

   // val selectExpression = movies.selectExpr("actor_name", "COUNT(movie_title) AS amount_of_movies".groupBy("actor_name")
    val moviesDS = movies.as[Movie]
    val normalSelect = moviesDS.select('actor_name, 'movie_title).groupBy('actor_name).count().as("actors_movies")

    val testSeq = Seq("test1", "asd2", "zxc3", "cos tam4")

    val testMap = testSeq.map(line => line.split(","))
    val testFlatMap = testSeq.flatMap(line => line.split(","))

    //testMap.foreach(println)
    testFlatMap.foreach(println)

//    movies.printSchema()
//    moviesSQL.show()
}
