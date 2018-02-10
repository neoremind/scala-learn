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

  ////////////////////

  def removeElements(head: ListNode, value: Int): ListNode = {
    if (head == null) return null
    head.next = removeElements(head.next, value)
    if (head._x == value) {
      head.next
    } else {
      head
    }
  }

  def removeElementsTailRec(head: ListNode, value: Int): ListNode = {
    val dummy = new ListNode(Integer.MAX_VALUE)
    dummy.next = head
    helper(dummy, value)
    dummy.next
  }

  @tailrec
  def helper(node: ListNode, value: Int): ListNode = {
    if (node == null) {
      null
    } else {
      var nextNotEqualToValue = node.next
      while (nextNotEqualToValue != null && nextNotEqualToValue._x == value) {
        nextNotEqualToValue = nextNotEqualToValue.next
      }
      node.next = nextNotEqualToValue
      helper(nextNotEqualToValue, value)
    }
  }

  class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x

    override def toString: String = {
      s"$x->${next}"
    }
  }

  val l1_1 = new ListNode(1)
  val l2_2 = new ListNode(2)
  val l3_6 = new ListNode(6)
  val l4_3 = new ListNode(3)
  val l5_4 = new ListNode(4)
  val l6_5 = new ListNode(5)
  val l7_6 = new ListNode(6)
  l1_1.next = l2_2
  l2_2.next = l3_6
  l3_6.next = l4_3
  l4_3.next = l5_4
  l5_4.next = l6_5
  l6_5.next = l7_6
  removeElements(l1_1, 6)
  removeElementsTailRec(l1_1, 6)
  println(l1_1)

}
