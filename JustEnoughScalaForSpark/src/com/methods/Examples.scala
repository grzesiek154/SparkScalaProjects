package com.methods

import java.io.File

object Examples {
  
   val shakespeare = new File("../data/shakespeare")
   val pathSeparator = File.separator
   val targetDirName = shakespeare.toString
   val plays = Seq("tamingoftheshrew", "comedyoferrors", "loveslabourslost", "midsummersnightsdream",
"merrywivesofwindsor", "muchadoaboutnothing", "asyoulikeit", "twelfthnight")
   
  def main(args: Array[String]) {
    
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
    
   
    
//    Scala's if construct is actually an expression (in Java they are statements). The if expression will return
//    true or false and assign it to success , which we'll use in a moment.
    
    val success = if (shakespeare.exists == false) {
        error(s"Data directory path does not exist! $shakespeare")
        false
    } else {
      info(s"$shakespeare exist")
      true
    }
    
    
    if(success) {
      println(s"Checking that the plays are in $shakespeare:")
      val failures = for {
        play <- plays
        playFileName = targetDirName + pathSeparator
        playFile = new File(playFileName)
        if (playFile.exists == false)
      } yield {
        s"$playFileName:\tNOT FOUND!"
      }
    
    println("finished")
    if(failures.size == 0) {
       info("All plays found")
    } else {
      println("The following expected plays were not found:")
      failures.foreach(play => error(play))
      }
    }
    
    
    
  }
}