package com.methods

object Lambda {
  
  
    val f = (x:Double) => x*x
    val sum = (a:Int, b:Int) => a+b
  
  def main(args: Array[String]) {
    
    
    println(f(2.4))
    println(sum(46,756))
    println(() => { "some text " + "anotehr text" })
    
  }
}