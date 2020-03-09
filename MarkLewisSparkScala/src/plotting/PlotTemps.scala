package plotting

import standardscala.TempData

object PlotTemps {
  
  
    val source = scala.io.Source.fromFile("MN212142_9392.csv")
    val lines = source.getLines().drop(1)
    val data = lines.flatMap { line =>
      val p = line.split(",")
      if(p(7)=="." || p(8)=="." || p(9)==".") Seq.empty else
      Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, 
          p(9).toDouble))
    }.toArray
    source.close()
}