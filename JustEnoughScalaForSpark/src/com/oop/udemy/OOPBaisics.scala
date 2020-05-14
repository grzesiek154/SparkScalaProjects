package com.oop.udemy

object OOPBaisics extends App{
  
  val person = new Person("Jan",28)
  println(person.age)
  val employee = new Employee("Ania", 25)
  println(employee.greet("Karolinka"))
  employee.greet()
}

class Person(name: String, val age: Int) //constructor

// class parameters are NOT FIELDS
// in order to convert parameter to the filed we need to add "val" keyword

class Employee(name: String, val age: Int) {
  
  
  //multpile constructots
  def this(name: String) = this(name, 0)
  def this() = this("John Doe")
  
  val x = 2
  
  println(1 + 3)
  // every instantiation of a class evaluates block of code inside this class
  
  
  def greet(name: String): Unit = {
    println(s"${this.name} says: Hi , $name")
  }
  
  //overloading - defining method with the same name, but with different signatures
  def greet() = println(s"Siema, I am $name")
  // def greet() = println(123) - if we define methods with the same signature but with different result
  //it compliller will throw an error
  
}