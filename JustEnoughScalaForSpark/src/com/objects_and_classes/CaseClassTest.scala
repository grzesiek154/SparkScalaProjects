package com.objects_and_classes

case class CaseClassTest(
  word:        String,
  total_count: Int           = 0,
  locations:   Array[String] = Array.empty,
  counts:      Array[Int]    = Array.empty) {
  // dodawanie domyslnych warotsci dla parametrow konstruktora, pozwala ominac te wartosci przy inicjalizacj instancji klasy
  /**
   * Different than our CSV output above, but see toCSV.
   * Array.toString is useless, so format these ourselves.
   */
  override def toString: String =
    s"""IIRecord($word, $total_count, $locStr, $cntStr)"""
    
  /** CSV-formatted string, but use [a,b,c] for the arrays */
  def toCSV: String =
    s"$word,$total_count,$locStr,$cntStr"
    
  /** Return a JSON-formatted string for the instance. */
  def toJSONString: String =
    s"""{
      | "word": "$word",
      | "total_count": $total_count,
      | "locations": ${toJSONArrayString(locations)},
      | "counts" ${toArrayString(counts, ", ")}
      |}
      |""".stripMargin

      private def locStr = toArrayString(locations)
      private def cntStr = toArrayString(counts)
      
      // "[_]" means we don't care what type of elements; we're just
// calling toString on them!
      private def toArrayString(array: Array[_], delim: String = ",") : String = 
        array.mkString("[", delim, "]")
        
      private def toJSONArrayString(array: Array[String]): String = 
          toArrayString(array.map(quote), ",")
      private def quote(word: String): String = "\"" + word + "\""    
        
//      Okay, what about that case keyword? It tells the compiler to do several useful things for us, eliminating a lot of
//boilerplate that we would have to write for ourselves with other languages, especially Java:
//1.Treat each constructor argument as an immutable ( val ) private field of the instance.
//2. Generate a public reader method for the field with the same name (e.g., word ).

//3.Generate correct implementations of the equals and hashCode methods, which people often implement
//incorrectly, as well as a default toString method. You can use your own definitions by adding them
//explicitly to the body. We did this for toString , to format the arrays in a nicer way than the default
//Array[_].toString method.
//4. Generate an object IIRecord , i.e., with the same name. The object is called the companion object.

//5.Generate a "factory" method in the companion object that takes the same argument list and instantiates an
//instance.
//6. Generate helper methods in the companion object that support pattern matching.
//Points 1 and 2 make each argument behave as if they are public, read-only fields of the instance, but they are
//actually implemented as described.
//Point 3 is important for correct behavior. Case class instances are often used as keys in Maps (http://www.scalalang.
//org/api/current/index.html#scala.collection.Map) and Sets (http://www.scala-lang.org/api/current
///index.html#scala.collection.Set), Spark RDD and DataFrame methods, etc. In fact, you should only use your
//case classes or Scala's built-in types with well-defined hashCode and equals methods (like Int and other
//number types, String , tuples, etc.) as keys.
//For point 4, the companion object is generated automatically by the compiler. It adds the "factory" method
//discussed in point 5, and methods that support pattern matching, point 6. You can explicitly define these
//methods and others yourself, as well as fields to hold state. The compiler will still insert these other methods.
//However, see Ambiguities with Companion Objects. The bottom line is that you shouldn't define case classes in
//notebooks like this with extra methods in the companion object, due to parsing ambiguities.
//Point 5 means you actually rarely use new when creating instances.
}