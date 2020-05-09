package com.oop.udemy.exercises

abstract class MyList[+A] {

  def getListHead(): A
  def getListTail(): MyList[A]
  def isEmpty: Boolean
  def addItem[B >: A](item: B): MyList[B]
  def printElements: String
  override def toString: String = "[" + printElements + "]"
  

}

object Empty extends MyList[Nothing] {

  def getListHead(): Nothing = throw new NoSuchElementException
  def getListTail(): MyList[Nothing] = throw new NoSuchElementException
  def isEmpty: Boolean = true
  def addItem[B >:Nothing](item: B): MyList[B] = new Cons(item, Empty)
  def printElements: String = ""

}

class Cons[+A](h: A, t: MyList[A]) extends MyList[A] {
  def getListHead(): A = h
  def getListTail(): MyList[A] = t
  def isEmpty: Boolean = false
  def addItem[B >: A](item: B): MyList[B] = new Cons(item, this)
  def printElements: String = 
    if(t.isEmpty) "" + h
    else h + " " + t.printElements
}

object ListTest extends App {
  
    val listOfIntegers: MyList[Int] = new Cons(1, new Cons(2, new Cons(3, Empty)))
    val listOfStrings: MyList[String] = new Cons("Siema", new Cons("Scala",Empty))
}