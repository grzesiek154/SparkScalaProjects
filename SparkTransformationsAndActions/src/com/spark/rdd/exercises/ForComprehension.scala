package com.spark.rdd.exercises

object ForComprehension {
  
  
  def main(args: Array[String]) {
    
    //Use a yield statement with a for loop and your algorithm to create a new collection
    //from an existing collection.
    
    val names = Array("chris", "ed", "maurice")
    
    val capNames = for (name <- names) yield println(name.toUpperCase())
    //Using a for loop with a yield statement is known as a for comprehension.
    
    val lengths = for (e <- names) yield {
      // imagine that this required multiple lines of code
       e.length
     }
    
//    Except for rare occasions, the collection type returned by a for comprehension is the
//    same type that you begin with. For instance, if the collection you’re looping over is an
//    ArrayBuffer:
    
   // Discussion
//    If you’re new to using yield with a for loop, it can help to think of the loop like this:
//    • When it begins running, the for/yield loop immediately creates a new, empty
//    collection that is of the same type as the input collection. For example, if the input
//    type is a Vector, the output type will also be a Vector. You can think of this new
//    collection as being like a bucket.
//    • On each iteration of the for loop, a new output element is created from the current
//    element of the input collection. When the output element is created, it’s placed in
//    the bucket.
//    • When the loop finishes running, the entire contents of the bucket are returned.
  }
}