import java.io.{File, PrintWriter}

import scala.io.Source

/**
 * @author zhangxu
 */
object FileTest extends App {
  val writer = new PrintWriter(new File("test.txt"))
  writer.write("1")
  for (i <- 2 to 10) {
    writer.println(i)
  }
  writer.close()

  println("Following is the content read:")

  Source.fromFile("test.txt", "UTF-8").foreach {
    print
  }

  val tokens = Source.fromFile("test.txt").mkString.split("\\s+")
  val numbers = for (w <- tokens) yield w.toInt
  // 什么类型？
  val arr: Array[String] = Source.fromFile("test.txt").getLines().toArray
  for (i <- 0 until arr.length) {
    println(arr(i))
  }
}
