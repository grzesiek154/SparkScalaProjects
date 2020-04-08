package standardscala

object ParallelCollections extends App{
  

  
val list = (1 to 10000).toList

println(list.map(_ + 42))

println(list.par.map(_ + 42))


//Side-Effecting Operations
//
//Given the concurrent execution semantics of the parallel collections framework, operations performed on a collection which cause side-effects should generally be avoided, in order to maintain determinism. A simple example is by using an accessor method, like foreach to increment a var declared outside of the closure which is passed to foreach

var sum = 0
list.par.foreach(sum += _)
println(sum)

//Here, we can see that each time sum is reinitialized to 0, and foreach is called again on list, sum holds a different value. The source of this non-determinism is a data race– concurrent reads/writes to the same mutable variable.
//
//In the above example, it’s possible for two threads to read the same value in sum, to spend some time doing some operation on that value of sum, and then to attempt to write a new value to sum, potentially resulting in an overwrite (and thus, loss) of a valuable result
 // parallel collections assigns values to different threads, threads that get smaller number tend to count and print values faster

//ThreadA: read value in sum, sum = 0                value in sum: 0
//ThreadB: read value in sum, sum = 0                value in sum: 0
//ThreadA: increment sum by 760, write sum = 760     value in sum: 760
//ThreadB: increment sum by 12, write sum = 12       value in sum: 12
//
//The above example illustrates a scenario where two threads read the same value, 0, before one or the other can sum 0 with an element from their partition of the parallel collection. In this case, ThreadA reads 0 and sums it with its element, 0+760, and in the case of ThreadB, sums 0 with its element, 0+12. After computing their respective sums, they each write their computed value in sum. Since ThreadA beats ThreadB, it writes first, only for the value in sum to be overwritten shortly after by ThreadB, in effect completely overwriting (and thus losing) the value 760.

//Non-Associative Operations
//
//Associative Operations to dzialania takie jak dodawanie albo mnozenie  liczb w sytuacji gdy zmiana kolejnosci lub dodanie nawiasow nie wplywa na wynik koncowy ?

println(list.par.reduce(_-_))
println(list.par.reduce(_-_))

//In the above example, we take a ParVector[Int], invoke reduce, and pass to it _-_, which simply takes two unnamed elements, and subtracts the first from the second. Due to the fact that the parallel collections framework spawns threads which, in effect, independently perform reduce(_-_) on different sections of the collection, the result of two runs of reduce(_-_) on the same collection will not be the same.




}