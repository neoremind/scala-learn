import java.util
import org.scalatest.Matchers._
import org.scalatest.FlatSpec

/**
 * @author zhangxu
 */
class SampleTest extends FlatSpec {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new util.Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)

    val xs = List(1, 2, 3, 4, 5)
    all(xs) should be > 0
    xs should have size 5
    xs should contain(5)
    xs(0) should equal(1)
    xs should not equal List(1)

    val string = "Hello seven world"
    string should startWith("Hello")
    string should endWith("world")
    string should include("seven")

    val a = 5
    val b = 2
    assertResult(3) {
      a - b
    }

    val s = "hi"
    try {
      s.charAt(-1)
      fail()
    }
    catch {
      case _: IndexOutOfBoundsException => // Expected, so continue
    }

    intercept[IndexOutOfBoundsException] {
      s.charAt(-1)
    }
  }

}
