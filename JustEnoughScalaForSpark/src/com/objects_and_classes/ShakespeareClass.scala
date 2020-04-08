package com.objects_and_classes

class ShakespeareClass(
    
//    Scala uses the same distinction between classes and instances that you find in Java. Classes are like templates
//used to create instances.
//We've talked about the types of things, like word is a String and totalCount is an Int . A class defines
//a type in the same sense.
    
//    When defining a class, the argument list after the class name is the argument list for the primary constructor. You
//can define secondary constructors, too, but it's not very common, in part for reasons we'll see shortly.
//Note that when you override a method that's defined in a parent class, like Java's Object.toString , Scala
//requires you to add the override keyword.
//We created an instance of IIRecord1 using new , just like in Java.
//Finally, as a side note, we've been using Ints (integers) all along for the various counts, but really for "big
//data", we should probably use Longs .
    
  word:        String,
  total_count: Int,
  locations:   Array[String],
  counts:      Array[Int]) {
  /** CSV formatted string, but use [a,b,c] for the arrays */
  override def toString: String = {
    val locStr = locations.mkString("[", ",", "]") // i.e., "[a,b,c]"
    val cntStr = counts.mkString("[", ",", "]") // i.e., "[1,2,3]"
    s"$word,$total_count,$locStr,$cntStr"
  }
}



  
