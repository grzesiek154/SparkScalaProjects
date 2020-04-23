package com.functions_udemy_tutorial

//All scala functions are objects because Scala is works on JVM which is designed to OOP
//Function types Function[A,B,R] === (A,B) => R

//=> is syntactic sugar for creating instances of functions. Recall that every function in scala is an instance of a class.
//For example, the type Int => String, is equivalent to the type Function1[Int,String] i.e. a function that takes an argument of type Int and returns a String.
//
//  scala> val f: Function1[Int,String] = myInt => "my int: "+myInt.toString
//  f: (Int) => String = <function1>
//  scala> f(0)
//  res0: String = my int: 0
//
//  scala> val f2: Int => String = myInt => "my int v2: "+myInt.toString
//  f2: (Int) => String = <function1>
//  scala> f2(1)
//  res1: String = my int v2: 1
//
//Here myInt is bound to the argument value passed to f and f2.
//
//() => T is the type of a function that takes no arguments and returns a T. It is equivalent to Function0[T]. () is called a zero parameter list I believe.
//
// scala> val f: () => Unit = () => { println("x")}
// f: () => Unit = <function0>
// scala> f()
// x
//
//scala> val f2: Function0[Unit] = () => println("x2")
//f: () => Unit = <function0>
//scala> f2()
//x2



object WhatsAFunction extends App {

  // we can call doubler like it were a function
  val doubler = new MyFunction[Int, Int] {
    override def apply(element: Int): Int = element * 2
  }

  println(doubler(2))
  
  //function types = Function[A,B]
  // new Function - supports up to 22 parameters 
  
  val stringToIntConvert = new Function1[String,Int] {
    override def apply(string: String): Int = string.toInt
  }
  
  println(stringToIntConvert("123") * 14)
  
  trait MyFunction[A, B] {
    def apply(element: A): B

  }
  
  val concatanate = ((word1:String, word2:String) => println( word1 +" " + word2 ))
  concatanate("Jan", "Kowalski")
  
  def concatenator: (String, String) => String = new Function2[String, String, String] {
    override def apply(a: String, b:String):String = a + b
  }
  
  // function that return a function
  def countSomething(value:Int) = (value2:Int) => {
    
    println("Final value equals: " + value * value2)
    
  }
  
  val firstVal = countSomething(20)
  
  val secondVal = firstVal(10)
}