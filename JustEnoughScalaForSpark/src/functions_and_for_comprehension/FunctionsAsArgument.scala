package com.grzesiek.spark.book.exercises
import java.io.File

object FunctionsAsArgument {
  
  
    def main(args: Array[String]) {
  
    val shakespeare = new File("../data/shakespeare")
    println(shakespeare)
    val pathSeparator = File.separator
    val targetDirName = shakespeare.toString
    val plays = Seq(
    "tamingoftheshrew", "comedyoferrors", "loveslabourslost", "midsummersnightsdream",
    "merrywivesofwindsor", "muchadoaboutnothing", "asyoulikeit", "twelfthnight")
  
        def info(message: String): String = {
      println(message)
      // The last expression in the block, message, is the return value.
      // "return" keyword not required.
      // Do no additional formatting for the return string.
      message
      }
    
             def error(message: String): String = {
    // Print the string passed to "println" and add a linefeed ("ln"):
    // See the next cell for an explanation of how the string is constructed.
    val fullMessage = s"""
    |********************************************************************
    |
    | ERROR: $message
    |
    |********************************************************************
    |""".stripMargin
    println(fullMessage)
    fullMessage
    } 
    
      val success = if (shakespeare.exists == false) { // doesn't exist already?
        error(s"Data directory path doesn't exist! $shakespeare") // ignore returned string
        false
        } else {
        info(s"$shakespeare exists")
        true
        }
        println("success = " + success)
        

         
      if (success) {
          println(s"Checking that the plays are in $shakespeare:")
          val failures = for {
          play <- plays
          playFileName = targetDirName + pathSeparator + play
          playFile = new File(playFileName)
          if (playFile.exists == false)
          } yield {
          s"$playFileName:\tNOT FOUND!"
          }
          println("Finished!")
          if (failures.size == 0) {
          info("All plays found!")
          } else {
          println("The following expected plays were not found:")
          failures.foreach(play => error(play))
          }
}
         
        
        
        println("\nUsing an anonymous function that calls println: `str => println(str)`")
        println("(Note that the type of the argument `str` is inferred to be String.)")
        
        plays.foreach(str => println(str))
        
        
        println("\nAdding the argument type explicitly. Note that the parentheses are require")
        plays.foreach((str: String) => println(str))
        
        println("\nWhy do we need to name this argument? Scala lets us use _ as a placeholder.")
        plays.foreach(println(_))
        
        println("\nFor longer functions, you can use {...} instead of (...).")
        println("Why? Because it gives you the familiar multiline block syntax with {...}")
        plays.foreach {
        (str: String) => println(str)
        }
        
        println("\nThe _ placeholder can be used *once* for each argument in the list.")
        println("As an assume, use `reduceLeft` to sum some integers.")
        val integers = 0 to 10 // Return a "range" from 0 to 10, inclusive
        //integers.reduceLeft((i,j) => i + j)
        val reducedIntegers = integers.reduceLeft(_+_)
        
        //integers.foreach((integer: Int) => println(integer + (integer + 1)))
       
       
     
        println(reducedIntegers)
        
         
    }         
}