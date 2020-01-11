package com.grzesiek.spark.book.exercises

object CollectionTransformationWithMap {
  
  def main(args: Array[String]) {
    
//    Rather than using the for/yield combination shown in the previous recipe, call the map
//method on your collection, passing it a function, an anonymous function, or method
//to transform each element. This is shown in the following examples, where each String
//in a List is converted to begin with a capital letter:
    
    val helpers = Vector("adam", "kim", "melissa")
    
    val caps = helpers.map(e => e.capitalize)
    
   //The next example shows that an array of String can be converted to an array of Int:
    val names = Array("Fred", "Joe", "Jonathan")
    
    val nameLength = names.map(_.length)
    
    //The map method comes in handy if you want to convert a collection to a list of XML elements:
    val nices = List("Aleka", "Christina", "Molly")
    
    val elems = nices.map(nice => <li>{nice}</li>)
    
//    A function that’s passed into map can be as complicated as necessary. An example in the
//    Discussion shows how to use a multiline anonymous function with map. When your
//    algorithm gets longer, rather than using an anonymous function, define the function
//    (or method) first, and then pass it into map:
    
    def plusOne(c: Char): Char = (c.toByte+1).toChar
    
    val asd ="ASD".map(plusOne)
    println(asd)
    
//    When writing a method to work with map, define the method to take a single parameter
//    that’s the same type as the collection. In this case, plusOne is defined to take a char,
//    because a String is a collection of Char elements. The return type of the method
//    can be whatever you need for your algorithm. For instance, the previous
//    names.map(_.length) example showed that a function applied to a String can return
//    an Int.
//    Unlike the for/yield approach shown in the previous recipe, the map method also works
//    well when writing a chain of method calls. For instance, you can split a String into an
//    array of strings, then trim the blank spaces from those strings:
    
     val s = " eggs, milk, butter, Coco Puffs "
     val items = s.split(",").map(_.trim) 
     
//     For simple cases, using map is the same as using a basic for/yield loop:
//     But once you add a guard, a for/yield loop is no longer directly equivalent to just a
//      map method call. If you attempt to use an if statement in the algorithm you pass to a
//      map method, you’ll get a very different result:
     val fruits = List("apple", "banana", "lime", "orange", "raspberry")
     
     val newFruits = fruits.map( f =>
         if (f.length < 6) f.toUpperCase)
         
         newFruits.foreach(println)
         
         //you could filter the result after calling map to clean up the result:
         newFruits.filter(_ != ())
//     But in this situation, it helps to think of an if statement as being a filter, so the correct
//     solution is to first filter the collection, and then call map:
         
        fruits.filter(_.length() < 6).map(_.toUpperCase()) 
  }
}