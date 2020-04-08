package functions_and_for_comprehension

object FunctionalPrograming extends App{
 
  
//The callback:() syntax defines a function that has no parameters. If the function
//had parameters, the types would be listed inside the parentheses.
//The => Unit portion of the code indicates that this method returns nothing
  def executeFunction(callback:() => Unit) {
    callback()
    }
  
  val sayHello = () => { println("Hello") }
  
  println(executeFunction(sayHello))
  
}