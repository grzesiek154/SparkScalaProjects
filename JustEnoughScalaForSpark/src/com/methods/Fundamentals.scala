package com.methods

object Fundamentals {
  
  
//  Scala follows a common object-oriented convention where the term method is used for a
//function that's attached to a class or instance. Unlike Java, at least before Java 8, Scala also has
//functions that are not associated with a particular class or instance
  
    def main(args: Array[String]) {
      
       def info(message: String): String = {
		        println(message)
		        // The last expression in the block, message, is the return value.
            // "return" keyword not required.
            // Do no additional formatting for the return string.
		        message
		    }
       
      info("Test")
      
      /*
* "error" takes a single String argument, prints a formatted error message,
* and returns the message.
*/
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
      
      error("Some Error")
      
        // Method definitions have the following elements, in order:
        //The def keyword.
        //The method's name ( error and info here).
        //The argument list in parentheses. If there are no arguments, the empty parentheses can be omitted. This is
        //common for toString and "getter"-like methods that simply return a field in an instance, etc.
        //A colon followed by the type of the value returned by the method. This can often be inferred by Scala, so it's
        //optional, but recommended for readibility by users!
        //An = (equals) sign that separates the method signature from the body.
        //The body in braces { ... } , although if the body consists of a single expression, the braces are optional.
        //The last expression in the body is used as the return value. The return keyword is optional and rarely
        //used.
        //Semicolons ( ; ) are inferred at the end of lines (in most cases) and rarely used
      
//      Look at the argument list for error . It is (message: String) , where message is the argument name and
//its type is String . This convention for type annotations, name: Type , is also used for the return type,
//error(...): String . Type annotations are required by Scala for method arguments. They are optional in
//most cases for the return type. We'll see that Scala can infer the types of many expressions and variable
//declarations
      
//An expression has a value, while a statement does not. Hence, when we assign an expression to
//a variable, the value the expression returns is assigned to the variable.
      
      
    }
    
  

}