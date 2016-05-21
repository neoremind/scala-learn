package spark

/**
 * @author zhangxu
 */
object SparkPi {

  def main(args: Array[String]) {
    val n = 100000000;
    val start = System.currentTimeMillis();
    val count = (1 to n).map { i =>
      val x = Math.random() * 2 - 1
      val y = Math.random() * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.par.reduce(_ + _)
    val pi = 4.0 * count / n
    val cost = System.currentTimeMillis() - start
    println("pi=" + pi + " ,cost " + cost + "ms")
  }

}
