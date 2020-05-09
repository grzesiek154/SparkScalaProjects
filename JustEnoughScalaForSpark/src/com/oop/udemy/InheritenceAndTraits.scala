package com.oop.udemy

object InheritenceAndTraits extends App{

  // scala alow single class inheritence
  sealed class Animal  {
    val creatureType = "wild"
    def eat = println("mniam")
  }

  class Cat extends Animal {
    def crunch = {
      eat
      println("crunch crunch")
      // protected allows to use methods in sublclass but not outside class
    }
  }

  val cat = new Cat
  cat.crunch

  // constructors
  class Person(name: String, age: Int) {
    def this(name: String) = this(name, 0)
  }
  class Adult(name: String, age: Int, idCard: String) extends Person(name)

  //overiding
  // we can overide fileds directly in the constructor
  class Dog(override val creatureType: String) extends Animal {
    //    override val creatureType = "domestic"
    override def eat = {
      super.eat
      println("crunch, crunch")
    }
  }
  
    //overiding
  class Dog2(animalType: String) extends Animal {
    override val creatureType = animalType
    override def eat = {
      super.eat
      println("crunch, crunch")
    }
  }
  val dog = new Dog("home animal")
  val dog2 = new Dog2("home animal")
  
  

  // type substitution (broad: polymorphism)
  val unknowAnimal: Animal = new Dog("K9")
  unknowAnimal.eat
  
  // overRIDING vs overLOADING

  // super

  // preventing overrides
  // 1 - use final on member
  // 2 - use final on the entire class
  // 3 - seal the class = extend classes in THIS FILE, prevent extension in other files

}