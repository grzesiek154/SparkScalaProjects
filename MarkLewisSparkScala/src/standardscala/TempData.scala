package standardscala


case class TempData(day: Int, doy: Int, month: Int, year: Int,
    precip: Double, snow: Double, tave:Double, tmax: Double, tmin: Double)

object TempData {
  
  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _:NumberFormatException => -1
    }
  }
  
  def main(args: Array[String]): Unit = {
    
    val source = scala.io.Source.fromFile("MN212142_9392.csv")
    val lines = source.getLines().drop(1) // usuwamy naglowki tabel

    //filtrujemy dane z pliku, aby usnunac wszstkie znaki kropki po i przed przcinkiem( ",.,")
    // flatMap returns sequence of data
    val data = lines.filterNot(_.contains(",.,")).flatMap { line =>
      val p = line.split(",")
      // sprawdzamy czy dane wartosci rowne sa "." 
      if(p(7)=="." || p(8)=="." || p(9)==".") Seq.empty else
       // zwracam seq danych 
      Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
        toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, 
          p(9).toDouble))
    }.toArray
    
    source.close()
 
    val maxTemp = data.map(_.tmax).max
    val hotDays = data.filter(_.tmax == maxTemp)
    println(s"Hot days are ${hotDays.mkString(", ")}")
    
    //maxBy zwraca najwieksz 
    val hotDay = data.maxBy(_.tmax)
    println(s"Hot day 1 is $hotDay")
    // reduceleft poczytac !!!!
    val hotDay2 = data.reduceLeft((d1, d2) => if(d1.tmax >= d2.tmax) d1 else d2)
    println(s"Hot day 2 is $hotDay2")
    
    val rainyCount = data.count(_.precip >= 1.0)
    println(s"There are $rainyCount rainy days. There is ${rainyCount*100.0/data.length} percent.")
    
    
    // opisac lepiej zmienne
    // foldLeft pozwala nam zwrocic inna wartosc niz zawiera kolekcja!!!!!
    // dwie wartosc przy val "val (rainySum, rainyCount2)" znacza ze zwracamy tuple  
    // parametr w foldLeft mozna zastapic wyrazeniem ((0.0,0))
    // (0.0, 0) znaczy ze wartoscia od ktorej zaczynamy jest 0.0 dla rauySum oraz 0 dla rainCount2
    val (rainySum, rainyCount2) = data.foldLeft(0.0 -> 0) {
      case ((sum, cnt), td) => 
        if(td.precip < 1.0) (sum, cnt) else (sum+td.tmax, cnt+1)
    }
    println(s"Average Rainy temp is ${rainySum/rainyCount2}")
    
    // flat map pozwala na zwrocenie listy wartosci lub pojedynczej wartosc
    val rainyTemps = data.flatMap(td => if(td.precip < 1.0) Seq.empty else Seq(td.tmax))
     println(s"Average Rainy temp is ${rainyTemps.sum/rainyTemps.length}")
     
     val monthGroups = data.groupBy(_.month)
     val monthlyTemp = monthGroups.map{
      case (months, days) => months -> days.foldLeft(0.0)((sum, td) => sum+td.tmax) / days.length
    }
    
    monthlyTemp.toSeq.sortBy(_._1) foreach println
  }
}