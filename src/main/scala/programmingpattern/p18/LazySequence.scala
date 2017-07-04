package programmingpattern.p18

import scala.collection.immutable.Stream._
import scala.util.Random


/**
  * 惰性序列模式
  *
  * 结合scala的特殊运算符 `#::`
  *
  * Created by xu.zhang on 7/2/17.
  */
object LazySequence extends App {

  val integers = Stream.from(0)

  println(integers take 5 foreach println)

  ////////////////////////////

  val generate = new Random()
  val randoms = Stream.continually(generate.nextInt)
  println(randoms take 5 foreach println)
  println("------")
  println(randoms take 6 foreach println) //前五个一样

  /////////
  // 分页的数据响应

  def pagedSequence(pageNum: Int): Stream[String] =
    getPage(pageNum) match {
      case Some(page: String) => page #:: pagedSequence(pageNum + 1)
      case None => Stream.Empty
    }

  def getPage(page: Int) =
    page match {
      case 1 => Some("Page1")
      case 2 => Some("Page2")
      case 3 => Some("Page3")
      case _ => None
    }

  println(pagedSequence(1) take 2 force)
  println(pagedSequence(1) force)

  ///////////////

  val holdsHead = {
    def pagedSequence(pageNum: Int): Stream[String] =
      getPage(pageNum) match {
        case Some(page: String) => {
          println("Realizing " + page)
          page #:: pagedSequence(pageNum + 1)
        }
        case None => Stream.Empty
      }

    pagedSequence(1)
  }

  def doesntHoldHead = {
    def pagedSequence(pageNum: Int): Stream[String] =
      getPage(pageNum) match {
        case Some(page: String) => {
          println("Realizing " + page)
          page #:: pagedSequence(pageNum + 1)
        }
        case None => Stream.Empty
      }

    pagedSequence(1)
  }

  val printHellos = Stream.continually(println("hello"))

  val aStream = "foo" #:: "bar" #:: Stream[String]()


}
