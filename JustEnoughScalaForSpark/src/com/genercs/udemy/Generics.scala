package com.genercs.udemy

object Generics extends App {

  class MyList[+A] {

    def add[B >: A](element: B): MyList[B] = ???

    /*
      A = Cat
      B = Animal
     */
    // if I add a dog to my list of cats it will be a list of animals, B is supertype of A
  }

  class MyMap[Key, Value]

  val listOfIntegers = new MyList[Int]
  val listOfStrings = new MyList[String]

  //generic Methods

  object MyList {
    def empty[A]: MyList[A] = ???
  }

  val emptyListOfIntegers = MyList.empty[Int]

  // variance problem
  class Animal
  class Cat extends Animal
  class Dog extends Animal

  // 1. yes, List[Cat] extends List[Animal] = COVARIANCE
  class CovariantList[+A]
  val animal: Animal = new Cat
  val animalList: CovariantList[Animal] = new CovariantList[Cat]
  // animalList.add(new Dog) ??? HARD QUESTION => we return a list of Animals

  //2. No = invariance
  class InvarianceList[A]
  //val invariantAnimalList: InvarianceList[Animal] = new InvarianceList[Cat] // false
  

  // 3. Hell, no! CONTRAVARIANCE
  class Trainer[-A]
  val trainer: Trainer[Cat] = new Trainer[Animal]

  // bounded types
  class Cage[A <: Animal](animal: A) // it means that A accept only suptybes of Animal
  val cage = new Cage(new Dog)

}