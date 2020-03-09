package standardscala

object TestScala {
  
  def main(args: Array[String]) {
    
  
    val arr = Array(2,3,5,7)
    
    val redu = arr.reduce(_+_)
    val reduLeft = arr.reduceLeft(_+_)
    val foldMethod = arr.fold(3)(_+_)
    
    println(redu)
    println(reduLeft)
    println(foldMethod)
  }
}