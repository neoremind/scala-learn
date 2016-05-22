package com.baidu.bes.pmp

import java.util
import org.scalatest.Matchers._
import org.scalatest.FlatSpec

/**
 * @author zhangxu
 */
class PMPOrderPriceCalcTest extends FlatSpec {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new util.Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)

    val xs = List(1, 2, 3, 4, 5)
    all(xs) should be > 0
  }

}
