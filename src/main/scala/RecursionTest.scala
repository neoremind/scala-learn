import scala.annotation.tailrec

/**
 * @author zhangxu
 */
object RecursionTest {

  def main(args: Array[String]) {
    println(quickSort(List(5, 7, 2, 3, 0, 8)))
    println(reverse("abc"))
    println(max(List(5, 7, 2, 3, 0, 8)))
    println(sum(List(5, 7, 2, 3, 0, 8)))
    println(factorial(10))
    println(factorial2(10))
  }

  def quickSort(xs: List[Int]): List[Int] = {
    if (xs.isEmpty) xs
    else
      quickSort(xs.filter(x => x < xs.head)) ::: xs.head :: quickSort(xs.filter(x => x > xs.head))
  }

  def reverse(xs: String): String =
    if (xs.length == 1) xs else reverse(xs.tail) + xs.head

  def max(xs: List[Int]): Int = {
    if (xs.isEmpty)
      throw new java.util.NoSuchElementException
    if (xs.size == 1)
      xs.head
    else
    if (xs.head > max(xs.tail)) xs.head else max(xs.tail)
  }

  def sum(xs: List[Int]): Int =
    if (xs.isEmpty) 0 else xs.head + sum(xs.tail)

  def factorial(n: Int): Int =
    if (n == 0) 1 else n * factorial(n - 1)

  def factorial2(n: Int): Int = {
    @tailrec
    def loop(acc: Int, n: Int): Int =
      if (n == 0) acc else loop(n * acc, n - 1)

    loop(1, n)
  }

}
