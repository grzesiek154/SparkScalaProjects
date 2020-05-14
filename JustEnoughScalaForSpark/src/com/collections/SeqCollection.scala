package com.collections

import scala.collection.LinearSeq

object SeqCollection extends App{
  
  
  val someSeq = IndexedSeq(1, 2, 4,6)
  
  val anotherSeq = LinearSeq(1, 2, 4,6)
  
  anotherSeq.apply(7)
  
  someSeq.apply(3)
  
  someSeq.foreach(println)
}