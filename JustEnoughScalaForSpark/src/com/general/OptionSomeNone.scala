package com.general
import org.apache.log4j._

object OptionSomeNone extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def doInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  //  Getting the value from an Option
  //The toInt example shows how to declare a method that returns an Option. As a con‐
  //sumer of a method that returns an Option, there are several good ways to call it and
  //access its result:
  //• Use getOrElse
  //• Use foreach
  //• Use a match expression

  val x = doInt("12").getOrElse(0)

  //  Because an Option is a collection with zero or one elements, the foreach method can
  //be used in many situations:

  doInt("12").foreach { i =>
    println(s"Got an int: $i")
  }

  //Another good way to access the toInt result is with a match expression:
  doInt("1") match {
    case Some(i) => println(i)
    case None    => println("That didn't work.")
  }

  //  Using Option with Scala collections
  //Another great feature of Option is that it plays well with Scala collections. For instance,
  //starting with a list of strings like this:
  val bag = List("1", "2", "foo", "3", "bar")
  //imagine you want a list of all the integers that can be converted from that list of strings.
  //By passing the toInt method into the map method, you can convert every element in
  //the collection into a Some or None value:
  bag.map(doInt)
  //res0: List[Option[Int]] = List(Some(1), Some(2), None, Some(3), None)
  //This is a good start. Because an Option is a collection of zero or one elements, you can
  //convert this list of Int values by adding flatten to map:
  bag.map(doInt).flatten
  //res1: List[Int] = List(1, 2, 3)
  //As shown in Recipe 10.16, “Combining map and flatten with flatMap”, this is the same
  //as calling flatMap:
  bag.flatMap(doInt)
  //res2: List[Int] = List(1, 2, 3)
  
  //The collect method provides another way to achieve the same result:
   bag.map(doInt).collect{case Some(i) => i}
}