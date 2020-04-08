import org.apache.spark.sql.SparkSession
import org.apache.log4j._



object TestObject {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CreatingDataFrameByReadingTextFile")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

     val httpClient = MyHttpClient

    val data = httpClient.get("https://ghoapi.azureedge.net/api/Indicator")
    val jsonData = spark.read.json("https://ghoapi.azureedge.net/api/Indicator")

    jsonData.schema
  }
}
