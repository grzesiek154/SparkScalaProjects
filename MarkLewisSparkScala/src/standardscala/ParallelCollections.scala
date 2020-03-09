package standardscala

object ParallelCollections extends App{
  
  val arr = Array(4, 2, 7, 3, 9, 1).par
   // (((((0-4)-2)-7)-3)-9)-1
  
  println(arr.foldLeft(0)(_-_))
  println(arr.foldRight(0)(_-_))
  println(arr.fold(0)(_-_))
}