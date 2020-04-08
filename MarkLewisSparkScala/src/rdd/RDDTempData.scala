package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import standardscala.TempData
import org.apache.log4j._

object RDDTempData{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    // app run on local cluster
    
    val sc = new SparkContext(conf)
    
    
    val lines = sc.textFile("MN212142_9392.csv").filter(!_.contains("Day"))
    
    val data = lines.flatMap { line =>
    val p = line.split(",")
    if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
      Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
        TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
        p(9).toDouble))
  }
    //finding max temperature
    println(data.max()(Ordering.by(_.tmax)))
    
    //finding max temperature
    println(data.reduce((td1, td2) => if(td1.tmax >= td2.tmax) td1 else td2))
    
    //rainy days count
    val rainyCount = data.filter(_.precip >= 1.0).count()
    println(s"There are $rainyCount rainy days. There is ${rainyCount * 100.0 / data.count()} percent.")
    
    
    
      val (rainySum, rainyCount2) = data.aggregate(0.0 -> 0)({ case ((sum, cnt), td) => 
      if(td.precip < 1.0) (sum, cnt) else (sum+td.tmax, cnt+1)
    }, { case ((s1, c1), (s2, c2)) =>
      (s1+s2, c1+c2)
    })
    println(s"Average Rainy temp is ${rainySum/rainyCount2}")
    
    val rainyTemps = data.flatMap(td => if(td.precip < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average Rainy temp is ${rainyTemps.sum/rainyTemps.count}")// count instead of length in normal scala
    
    val monthGroups = data.groupBy(_.month)
    val monthlyTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum+td.tmax)/days.size
    }
    monthlyTemp.sortBy(_._1) foreach println
    
    println("Stdev if highs: " +data.map(_.tmax).stdev())
    println("Stdev if highs: " +data.map(_.tmin).stdev())
    println("Stdev if highs: " +data.map(_.tave).stdev())
  }
}