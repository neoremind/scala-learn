package programmingpattern.p20

/**
  * 自定义控制流
  *
  * Created by xu.zhang on 7/3/17.
  */
object Choose extends App {

  def choose[E](num: Int, first: () => E, second: () => E, third: () => E) =
    if (num == 1) first()
    else if (num == 2) second()
    else if (num == 3) third()

  def test[E](expression: E) = expression

  def testTwice[E](expression: E) = {
    expression
    expression
  }

  def printTwice[E](expression: E) {
    println(expression)
    println(expression)
  }

  testTwice(println("hello")) //只打印一次，pass by value
  printTwice("222") //ugly....

  /////////////
  // pass by name，不会对表达式进行求值

  def testByName[E](expression: => E) = {
    expression
    expression
  }

  testByName(println("hello")) //打印两次

  def simplerChoose[E](num: Int, first: => E, second: => E, third: => E) =
    if (num == 1) first
    else if (num == 2) second
    else if (num == 3) third

  ///////////////
  // 计算平均时间，非常实用!!!!

  def timeRun[E](toTime: => E) = {
    val start = System.currentTimeMillis
    toTime
    System.currentTimeMillis - start
  }

  println(timeRun(Thread.sleep(1000)))

  def avgTime[E](times: Int, toTime: => E) = {
    val allTimes = for (_ <- Range(0, times)) yield timeRun(toTime)
    allTimes.sum / times
  }

  println(avgTime(5, Thread.sleep(200)))

  ////////////////
  // 在spark pipeline 里面用到的计时
  object Timer {
    /**
      * Measure the running time of a piece of code
      *
      * @param f          the code whose running time needs to be measured
      * @param handleTime a function to handle the measured time
      * @return the result of the original code
      */
    def timing[A](f: => A)(handleTime: Long => Any): A = {
      val t = System.nanoTime()
      val result = f
      handleTime(System.nanoTime() - t)
      result
    }
  }

  Timer.timing(Thread.sleep(1000)) {
    time => println("cost %s nano seconds".format(time))
  }
}
