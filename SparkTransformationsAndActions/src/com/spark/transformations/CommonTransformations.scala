package com.spark.transformations

import org.apache.spark.SparkContext

import org.apache.log4j._

object CommonTransformations {
  
  
  def main (args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val stringList = Array("Spark is awesome","Spark is cool")
    val sc = new SparkContext("local[*]", "CommonTransformations")
    val stringRDD = sc.parallelize(stringList)
    
    
//    The second most commonly used transformation is flatMap. Let’s say you want to
//transform the stringRDD from a collection of strings to a collection of words. The
//flatMap transformation is perfect for this use case
    val wordRDD = stringRDD.flatMap(line => line.split(" "))
    wordRDD.collect().foreach(println)
    
    
//    Another commonly used transformation is the filter transformation. It does what its
//name sounds like, which is to filter a dataset down to the rows that meet the conditions
//defined inside the given func.
//A simple example is to find out how many lines in the stringRDD contain the word
//awesome. 
    val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
    awesomeLineRDD.collect
    
    //Union
//    Unlike previous transformations that take a function as an argument, a union
//transformation takes another RDD as an argument, and it will return an RDD that
//combines the rows from both RDDs. This is useful for situations when there is a need to
//append some rows to an existing RDD. This transformation does not remove duplicate
//rows of the resulting RDD
    
    val rdd1 = sc.parallelize(Array(1,2,3,4,5))
    val rdd2 = sc.parallelize(Array(9,8,7,6,0))
    val rdd3 = rdd1.union(rdd2)
    
    rdd3.foreach(println)
    
//    Insertion
//    If there were two RDDs and there is a need to find out which rows exist in both of them,
//then this is the right transformation to use. The way this transformation figures out
//which rows exist in both RDDs is by comparing their hash codes. This transformation
//guarantees the returned RDD will not contain any duplicate rows. Unlike the map and
//filter transformations, the implementation of this transformation moves rows with the
//same hash code to the same executor to perform the intersection. 
    
    val rdd4 = sc.parallelize(Array("One", "Two","Three"))
    val rdd5 = sc.parallelize(Array("two","One","threed","One","Three"))
    val rdd6 = rdd4.intersection(rdd5)
    
    rdd6.collect.foreach(println)
    
//    substract(otherRDD)
//    A good use case for this transformation is when there is a need to compute the statistics
//of word usage in a certain book or a set of speeches. A typical first task in this process is
//to remove the stop words, which refers to a set of commonly used words in a language. In
//the English language, examples of stop words are is, it, the, and and. So, if you have one
//RDD that contains all the words in a book and another RDD that contains just the list of
//stop words, then subtracting the first one from the second one will yield another RDD
//that contains only nonstop words
    
    val words = sc.parallelize(List("The amazing thing about spark is that it is very simple to learn"))
                                    .flatMap(line => line.split(" "))
                                    .map(word => word.toLowerCase())
    
    val stopWords = sc.parallelize(List("the it is to that"))
                    .flatMap(line=> line.split(" "))
    val realWords = words.subtract(stopWords)
    
    println("substract tranformation !!!!!!!!!!!")
    realWords.collect().foreach(println)
    
    
//    distinct( )
//The distinct transformation represents another flavor of transformation where it
//doesn’t take any function or another RDD as an input parameter. Instead, it is a directive
//to the source RDD to remove any duplicate rows. The question is, how does it determine
//whether two rows are the same? A common approach is to transpose the content of each
//row into a numeric value by computing the hash code of the content. That is exactly what
//Spark does. To remove duplicate rows in an RDD, it simply computes the hash code of
//each row and compares them to determine whether two rows are identical.
    val duplicateValueRDD = sc.parallelize(List("one", 1,"two", 2, "three", "one", "two", 1, 2))
    println("distinct tranformation starts !!!!!!!!!!!")
    duplicateValueRDD.distinct().collect().foreach(println)
    
    
    
//    sample(withReplacement, fraction, seed)
//Sampling is a common technique used in statistical analysis or machine learning to
//either reduce a large dataset to a more manageable size or to split the input dataset
//into a training set and a validation set when training a machine learning model.
//This transformation performs the sampling of the rows in the source RDD based
//on the following three inputs: with replacement, fraction, and seed values. The
//withReplacement parameter determines whether an already sampled row will be placed
//back into RDD for the next sampling. If the withReplacement parameter value is true,
//it means a particular row may appear multiple times in the output. The given fraction
//value must be between 0 and 1, and it is not guaranteed that the returned RDD will have
//the exact fraction number of rows of the original RDD. The optional seed parameter is
//used to seed the random generator, and it has a default value if one is not provided  
// The example in Listing 3-30 first creates an RDD with ten numbers, which are placed
//in two partitions; then it will try to sample the withReplacement value as true and the
//fraction as 0.3.    
    
    val numbers = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 2)
    println("sample transformation !!!!!!!!!!!!!!!")
    numbers.sample(true,0.5).collect.foreach(println)
    
  }

    
 //mapPartitionWithIndex and mapPartition   
//    One small difference between the mapPartitionWithIndex and mapPartition
//transformations is that the partition number is available to the former transformation.
//In short, the mapPartitions and mapPartitionsWithIndex transformations are used
//to optimize the performance of your data processing logic by reducing the number of
//times the expensive setup step is called.
    
    val sampleList = Array("One", "Two", "Three", "Four","Five")
    val sampleRDD = sc.parallelize(sampleList, 2)
//    val result = sampleRDD.mapPartitions((itr:Iterator[String]) => {
//                val rand = new Random(System.currentTimeMillis +
//                Random.nextInt)
//                itr.map(l => l + ":" + rand.nextInt)
//    })
    
   
    
    
     def addRandomNumber(rows:Iterator[String]) = {
      val rand = new Random(System.currentTimeMillis + Random.nextInt)
      rows.map(l => l + " : " + rand.nextInt)
      
    }
    
    val result = sampleRDD.mapPartitions((rows: Iterator[String]) => addRandomNumber(rows))
    
    println("MapPartition transformation")
    result.collect.foreach(println)
    
  }
  

}