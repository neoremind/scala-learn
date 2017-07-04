package programmingpattern.p9

/**
  * 替代装饰模式
  */
object DecoratorTest extends App {

  def add(a: Int, b: Int) = a + b

  def subtract(a: Int, b: Int) = a - b

  def multiply(a: Int, b: Int) = a * b

  def divide(a: Int, b: Int) = a / b

  def makeLogger(calcFn: (Int, Int) => Int) =
    (a: Int, b: Int) => {
      val result = calcFn(a, b)
      println("Result is: " + result)
      result
    }

  val loggingAdd = makeLogger(add)
  val loggingSubtract = makeLogger(subtract)
  val loggingMultiply = makeLogger(multiply)
  val loggingDivide = makeLogger(divide)

  println(loggingAdd(3, 4))
  println(loggingSubtract(3, 4))
  println(loggingMultiply(3, 4))
  println(loggingDivide(3, 4))

}
