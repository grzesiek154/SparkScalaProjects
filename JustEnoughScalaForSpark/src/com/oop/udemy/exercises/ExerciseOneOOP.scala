package com.oop.udemy.exercises

object ExerciseOneOOP  extends App{
  
  val author = new Writer("Jan", "Kowalski", 1980)
  println(author.fullName())
  
  val counnter = new Counnter(30)
  
  counnter.decrementVal(10).print()
  
}

 class Writer(name: String, surname:String,val yearOfBirth:Int) {
  
  def fullName():String =  s"${this.name} + ${this.surname}"
  
}

class Novel(name: String, year:Int, author: Writer) {
  
    def getAuthorAge():Int = this.author.yearOfBirth
    def isWrittenBy(author: Writer):Boolean = {
        if(this.author == author)  true else false
    }
    
    def copy (newYearOfRelease: Int):Novel = {
      new Novel(this.name, newYearOfRelease, this.author)
    }
}

class Counnter(someVal: Int) {
  
  def getCurrentCount():Int = someVal
  
  def incrementVal = {
    println("incrementing")
    new Counnter(someVal + 1)
  }
  
  def decrementVal = {
    println("decrementing")
    new Counnter(someVal - 1)// by instatiating new object we use IMUTABILITY of an object
  }
  
  def incrementVal(n: Int): Counnter = { if(n<=0) this else incrementVal.incrementVal(n - 1) }
  
  def decrementVal(n: Int): Counnter =  { if(n<=0) this else decrementVal.decrementVal(n - 1) }
  // we can do "decrementVal.decrementVal" because everything in scala is an expression, so first method
  // decrementVal returns "new Counter", from which we can call another overloaded method decrementVal
  
  def print() = println(someVal)
}