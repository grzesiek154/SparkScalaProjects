import org.apache.log4j._
import org.apache.spark.sql.SparkSession

case class SuperHero (id:Long, name:String)
case class HeroCoOccurrences (id:Long, others:Seq[Long])

object RddPlayground extends  App {

  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("DataFramesTransformations")
    .getOrCreate()

  def countCoOccurrences(line: String) = {
    var elements = line.split("\\s+")
    HeroCoOccurrences( elements(0).toInt, Seq(elements.length - 1 ))
  }

  def getHeroIdAndName(line:String): Option[(Long, String)] = {
    val elements = line.split('\"')
      if(elements.length > 1) {
         Some((elements(0).trim.toInt, elements(1)))
      } else {
         None
      }
  }

  def doFilter(name: String) = {
      if(name.contains("YC")) name

  }


  //marvel-graph - superhero id and list of superheros with whom he appeared
  //marvel-names - id and superhero name
  val marvelData = spark.sparkContext.textFile("../data/Marvel-names.txt").flatMap(line => getHeroIdAndName(line))
  val heroCoOccurrences = spark.sparkContext.textFile("../data/Marvel-graph.txt").map(line => countCoOccurrences(line))

  val filterTransformation = marvelData.filter(hero => hero._2.contains("YC"))



   //marvelData.foreach(println)
  //filterTransformation.foreach(println)

   filterTransformation.foreach(println)


}
