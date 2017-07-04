/**
  * In Scala, we talk about object-functional programming often. What does that mean?
  * What is a Function, really?
  * A Function is a set of traits. Specifically, a function that takes one argument
  * is an instance of a Function1 trait. This trait defines the apply() syntactic sugar
  * we learned earlier, allowing you to call an object like you would a function.
  *
  * There is Function0 through 22. Why 22? It’s an arbitrary magic number.
  * I’ve never needed a function with more than 22 arguments so it seems to work out.
  *
  * [[https://twitter.github.io/scala_school/basics2.html]]
  */
object Function1Test extends App {
  val succ = (x: Int) => x + 1
  val anonfun1 = new Function1[Int, Int] {
    def apply(x: Int): Int = x + 1
  }

  class AddOne extends (Int => Int) {
    def apply(m: Int): Int = m + 1
  }

  class AddOne2 extends Function1[Int, Int] {
    def apply(m: Int): Int = m + 1
  }

  assert(succ(0) == anonfun1(0))
  assert(succ(0) == new AddOne().apply(0))
  assert(succ(0) == new AddOne2().apply(0))
}
