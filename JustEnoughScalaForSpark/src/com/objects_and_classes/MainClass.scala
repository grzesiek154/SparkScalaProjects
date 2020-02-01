package com.objects_and_classes

object MainClass {
    
  
  def main(args: Array[String]) {
    //    val classTest = new ShakespeareClass("hello", 3, Array("one", "two"), Array(1,2))
    //
    //    println(classTest.toString())

    val hello = new CaseClassTest("hello")
    val world = CaseClassTest("world!", 3, Array("one", "two"), Array(1, 2))
//    What actually happens in the second case, without new ? The "factory" method is actually called apply . In
//Scala, whenever you put an argument list after any instance, including these objects , as in the hello2
//case, Scala looks for an apply method to call. The arguments have to match the argument list for apply
//(number of arguments, types of arguments, accounting for default argument values, etc.). Hence, the hello2
//declaration is really this:
    //val hello2 = CaseClassTest.apply("asdzxc")
    println("\n`toString` output:")
    println(hello)
    println(world)
    println("\n`toJSONString` output:")
    println(hello.toJSONString)
    println(world.toJSONString)
    println("\n`toCSV` output:")
    println(hello.toCSV)
    println(world.toCSV)
  }
}