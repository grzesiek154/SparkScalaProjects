package standardscala

object ParallelCollections {
  
  val arr = Array(4, 2, 7, 3, 9, 1)
   // (((((0-4)-2)-7)-3)-9)-1
  
  println(arr.foldLeft(0)(_+_))
}