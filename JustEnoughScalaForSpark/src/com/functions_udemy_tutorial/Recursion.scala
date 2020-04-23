package com.functions_udemy_tutorial

import scala.annotation.tailrec

// stack overflow error happen when the recursive depth is too big

object Recursion extends App{
  
  def factorial(n: Int): Int = {
    if (n <= 1) 1
    else {
      println("Computing factorial of " + n + " - I first need factorial of " + (n-1))
      val result = n + factorial(n - 1)
        println("Computed factorial of " + n)
        result
    }
  }
  
  def anotherFactorial(n: Long): Long = {
    //@tailrec - if a function in not tail recursive a compiler will throw an error
    @tailrec
    def factHelper(x: Long, accumulator: Long): Long = 
       if (x <= 1) accumulator
       else factHelper(x-1, x * accumulator) 
       
       factHelper(n, 1) // TAIL RECURSION = use recursive call as the LAST expression
       
       // why above works?
       //Scala doesn't need to save intermediate results to be used later
       // so when we evaluate this recursive call the current stack frame eg. factHelper of 10 is replaced
       // with facHelper of something elese without using extra stack memory
    }
  
   /*
    anotherFactorial(10) = factHelper(10, 1)
    = factHelper(9, 10 * 1)
    = factHelper(8, 9 * 10 * 1)
    = factHelper(7, 8 * 9 * 10 * 1)
    = ...
    = factHelper(2, 3 * 4 * ... * 10 * 1)
    = factHelper(1, 1 * 2 * 3 * 4 * ... * 10)
    = 1 * 2 * 3 * 4 * ... * 10
   */
  
  println(anotherFactorial(20))
  
  
  // WHEN YOU NEED LOOPS, USE _TAIL_ RECURSION.

  /*
    1.  Concatenate a string n times
    2.  IsPrime function tail recursive
    3.  Fibonacci function, tail recursive.
   */
  
//  def fibonacci(n: Int): Int = {
//    
//    def helperFun(x:Int, accOne: Int, accTwo: Int): Int = {
//      val result = 0;
//      if (n <= 2)  result
//      else 
//    }
//    helperFun(n, 2, 1)
//  }
  
  def stringConcatanate(someString: String, n:Int, accumulator: String): String = {
    if(n <= 0) accumulator 
    else stringConcatanate(someString, n-1, accumulator + " " + someString)
  }
  
  println(stringConcatanate("siema", 3, ""))
  
  def isPrime(n: Int):Boolean = {
    
    @tailrec
    def isPrimeTailRec(t: Int, isStillPrime: Boolean): Boolean = 
      if (!isStillPrime) false
      else if (t <= 1) true
      else isPrimeTailRec(t - 1, n % t != 0 && isStillPrime)
      
      isPrimeTailRec(n/2, true)
      
  }
  
  
  def fibonacci(n: Int): Int = {
    def fiboTailrec(i: Int, last: Int, nextToLast: Int): Int =
      if(i >= n) last
      else fiboTailrec(i + 1, last + nextToLast, last)

    if (n <= 2) 1
    else fiboTailrec(2, 1, 1)
  }
}