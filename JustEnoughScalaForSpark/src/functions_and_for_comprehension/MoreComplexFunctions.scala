package functions_and_for_comprehension

object MoreComplexFunctions extends App {

  //  Problem
  //You want to define a method that takes a function as a parameter, and that function may
  //have one or more input parameters, and may also return a value

  def exec(callback: Int => Unit) {
    callback(2)
  }

  //  Next, define a function that matches the expected signature. The following plusOne
  //function matches that signature, because it takes an Int argument and returns nothing:

  val plusOne = (x: Int) => { println(x + 1) }

  exec(plusOne)

  //  Although these examples are simple, you can see the power of the technique: you can
  //easily swap in interchangeable algorithms. As long as your function signature matches
  //what your method expects, your algorithms can do anything you want. This is compa‐
  //rable to swapping out algorithms in the OOP Strategy design pattern.

  //  Therefore, to define a function that takes a String and returns an Int, use one of these
  //two signatures:
  //executeFunction(f:(String) => Int)
  //// parentheses are optional when the function has only one parameter

  //executeFunction(f:String => Int)

  //To define a function that takes two Ints and returns a Boolean, use this signature:

  //executeFunction(f:(Int, Int) => Boolean)

  //The following exec method expects a function that takes String, Int, and Double
  //parameters and returns a Seq[String]:

  // def exec2(f:(String, Int, Double) => Seq[String])

  //  Passing in a function with other parameters
  //A function parameter is just like any other method parameter, so a method can accept
  //other parameters in addition to a function.
  val sayHello = () => println("Hello")

  def executeXTimes(callback: () => Unit, numTimes: Int) {

    for (i <- 1 to numTimes) callback()
  }

  executeXTimes(sayHello, 5)

  //  This method is more interesting than the previous method, because it takes the Int
  //parameters it’s given and passes those parameters to the function it’s given
  def executeAndPrint(f: (Int, Int) => Int, x: Int, y: Int) {
    val result = f(x, y)
    println(result)
  }

  val sum = (x: Int, y: Int) => x + y
  val multiply = (x: Int, y: Int) => x * y

  executeAndPrint(sum, 5, 98)
  executeAndPrint(multiply, 2, 32)

  //  This is cool, because the executeAndPrint method doesn’t know what algorithm is
  //actually run. All it knows is that it passes the parameters x and y to the function it is
  //given and then prints the result from that function. This is similar to defining an in‐
  //terface in Java and then providing concrete implementations of the interface in multiple
  //classes.

  def exec3(callback: (Any, Any) => Unit, x: Any, y: Any) {
    callback(x, y)
  }

  val printTwoThings = (a: Any, b: Any) => {
    println(a)
    println(b)

  }

  case class Person(name: String)
  exec3(printTwoThings, "Siema", Person("Janek"))
}