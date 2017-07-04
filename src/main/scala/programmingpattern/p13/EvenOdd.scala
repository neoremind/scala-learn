package programmingpattern.p13

import scala.util.control.TailCalls.TailRec
import scala.util.control.TailCalls._

/**
  * 相互递归模式
  *
  * Created by xu.zhang on 6/30/17.
  */
object EvenOdd extends App {

  def isOdd(n: Long): Boolean = if (n == 0) false else isEven(n - 1)

  def isEven(n: Long): Boolean = if (n == 0) true else isOdd(n - 1)

  def isOddTrampoline(n: Long): TailRec[Boolean] =
    if (n == 0) done(false) else tailcall(isEvenTrampoline(n - 1))

  def isEvenTrampoline(n: Long): TailRec[Boolean] =
    if (n == 0) done(true) else tailcall(isOddTrampoline(n - 1))

  println(isEven(0))
  println(isOdd(1))
}
