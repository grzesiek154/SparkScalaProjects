package functions_and_for_comprehension

class Foo {
  // a method that takes a function and a string, and passes the string into
  // the function, and then executes the function
  def exec(f: (String) => Unit, name: String) {
    f(name)
  }
}

object UsingClosures extends App{

  //  Problem
  //You want to pass a function around like a variable, and while doing so, you want that
  //function to be able to refer to one or more fields that were in the same scope as the
  //function when it was declared.
  
  var hello = "Czesc"
  def sayHello(name: String) { println(s"$hello, $name") }
  
  val foo = new Foo
  
  foo.exec(sayHello, "Karolina")
  
  hello = "Holla"
  foo.exec(sayHello, "Lorenzo")
  
//  If you’re coming to Scala from Java or another OOP language, you might be asking,
//“How could this possibly work?” Not only did the sayHello method reference the vari‐
//able hello from within the exec method of the Foo class on the first run (where hello
//was no longer in scope), but on the second run, it also picked up the change to the hello
//variable (from Hello to Hola). The simple answer is that Scala supports closure func‐
//tionality, and this is how closures work.
//As Dean Wampler and Alex Payne describe in their book Programming Scala (O’Reilly),
//there are two free variables in the sayHello method: name and hello. The name variable
//is a formal parameter to the function; this is something you’re used to.
//However, hello is not a formal parameter; it’s a reference to a variable in the enclosing
//scope (similar to the way a method in a Java class can refer to a field in the same class).
//Therefore, the Scala compiler creates a closure that encompasses (or “closes over”)
//hello
}