/**
  * Created by xu.zhang on 7/3/17.
  */
object CurriedFunctionsTest extends App {

  def multiply(m: Int)(n: Int): Int = m * n

  println(multiply(2)(3))

  val timesTwo = multiply(2) _

  println(timesTwo(3))
}
