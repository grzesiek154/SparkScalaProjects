package com.oop.udemy

object Objects extends App{
  
  //scala does not have a class level functionality ("static")
  // we canoc access fields that belongs to the class not the the instance
  
  
  object Person {
    // objects can have the same membes like class but do not receive parameters
    // scala object is a SINGELTON instance
    // scala objects behave like the would have "static" or "class" level functionaity becuase the are singelotns
    val N_EYES = 2
    def canFly:Boolean = true
      // factory method
    // it is called factory method because it's sole purpose is to build persons with given parameter
    def apply(mother: Person, father: Person): Person = new Person("Bobbie")
  }
  
  class Person(name: String) {
    // instance level functionality
  }
  
  // writing object and class with the same name is called COMPANIONS !!!
  //BECAUSE HAVE THE SAME SCOPE AND THE SAME NAME
  // companion can access each other private members
  
  println(Person.N_EYES)
  
      // Scala object = SINGLETON INSTANCE
    val mary = new Person("Mary")
    val john = new Person("John")
    println(mary == john)// false
    
    val person1 = Person
    val person2 = Person
    println(person1 == person2)// true
  
  val bobbie = Person(mary, john)// lok like constructor but it is apply method from singelton object
  
  // Scala Applications = Scala object with
  // def main(args: Array[String]): Unit
}