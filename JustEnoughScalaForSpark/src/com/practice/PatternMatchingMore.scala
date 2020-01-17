package com.practice

import java.io.File
import org.apache.spark.SparkContext
import org.apache.log4j._

object PatternMatchingMore {

  def shakespeareDataManipulation() {

    val shakespeare = new File("../data/shakespeare")
    val sc = new SparkContext("local", "WordsCountInvertIndex")
    val pathSeparator = File.separator

    val fileContent = sc.wholeTextFiles(shakespeare.toString).flatMap {

      case (location, "") =>
        Array.empty[((String, String), Int)]
      case (location, contents) =>
        val words = contents.split("""\W+""")
        val fileName = location.split("\\\\").last
        words.map(word => ((word, fileName), 1))
      //          Pattern matching is eager. The first successful match in the order as written will win. If you reversed the order
      //here, the case (location, "") would never match and the compiler would throw an "unreachable code"
      //warning for it. czyli jesli pierwszy case wyszukla by nam wszystkie wartosci rowneiz te puste, drugi case
      // nic by nie znalazl

    }

    fileContent.foreach(println)
  }

  def casePatternTest() = {

    val stuff = Seq(1, 3.14159, 2L, 4.4F, ("one", 1), (404F, "boo"), ((11, 12), 21, 31))

    stuff.foreach {
      case i: Int    => println(s"Found an Int: $i")
      case l: Long   => println(s"Found a Long: $l")
      case f: Float  => println(s"Found a Float: $f")
      case d: Double => println(s"Found a Double: $d")

      case (x1, x2) =>
        println(s"Found a two-element tuple with elements of arbitrary type: ($x1, $x2)")
      case ((x1a, x1b), _, x3) =>
        println(s"Found a three-element tuple with 1st and 3th elements: ($x1a, $x1b) and $x3")
      case default => println(s"Found something else: $default")

    }
    
//        A few notes.
//    A literal like 1 is inferred to be Int , while 3.14159 is inferred to be Double . Add L or F ,
//    respectively, to infer Long or Float instead.
//    Note how we mixed specific type checking, e.g., i: Int , with more loosely-typed expressions, e.g.,
//    (x1, x2) , which expects a two-element tuple, but the element types are unconstrained.
//    All the words i , l , f , d , x1 , x2 , x3 , and default are arbitrary variable names. Yes default is
//    not a keyword, but an arbitrary choice for a variable name. We could use anything we want.
//    The last default clause specifies a variable with no type information. Hence, it matches anything, which
//    is why this clause must appear last. This is the idiom to use when you aren't sure about the types of things
//    you're matching against and you want to avoid a possible MatchError (http://www.scala-lang.org/api/current
//    /index.html#scala.MatchError).
//    If you want to match that something exists, but you don't need to bind it to a variable, then use _ , as in the
//    three-element tuple example.
//    The three-element tuple example also demonstrates that arbitrary nesting of expressions is supported,
//    where the first element is expected to be a two-element tuple.
    
    
//        All the anonymous functions we've seen that use these pattern matching clauses have this format:
//    {
//    case firstCase => ...
//    case secondCase => ...
//    ...
//    }
//    This format has a special name. It's called a partial function. All that means is that we only "promise" to accept
//    arguments that match at least one of our case clauses, not any possible input.
//    The other kind of anonymous function we've seen is a total function, to be precise.
//    Recall we said that for total functions you can use either (...) or {...} around them, depending on the
//    "look" you want. For partial functions, you must use {...}
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //shakespeareDataManipulation()

    casePatternTest()
  }
}