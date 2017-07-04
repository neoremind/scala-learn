package programmingpattern.p12

import scala.annotation.tailrec

/**
  * 尾递归模式
  *
  * Created by xu.zhang on 6/30/17.
  */
object TailRecursion extends App {

  case class Person(firstName: String, lastName: String)

  def makePeople(firstNames: Seq[String], lastNames: Seq[String]) = {

    @tailrec
    def helper(firstNames: Seq[String], lastNames: Seq[String], people: Vector[Person]): Seq[Person] = {
      if (firstNames.isEmpty) {
        people
      } else {
        helper(firstNames.tail, lastNames.tail, people :+ Person(firstNames.head, lastNames.head))
      }
    }

    helper(firstNames, lastNames, Vector[Person]())
  }

  val firstNames = Seq("a", "b", "c", "d")
  val lastNames = Seq("1", "2", "3", "4")
  println(makePeople(firstNames, lastNames))

}
