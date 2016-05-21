/**
 * @author zhangxu
 */
object ListsTest {

  // List of Strings
  val fruit: List[String] = List("apples", "oranges", "pears")

  // List of Integers
  val nums: List[Int] = List(1, 2, 3, 4)

  // Empty List.
  val empty: List[Nothing] = List()

  // Two dimensional list
  val dim: List[List[Int]] =
    List(
      List(1, 0, 0),
      List(0, 1, 0),
      List(0, 0, 1)
    )

  // List of Strings
  val fruit2 = "apples" :: ("oranges" :: ("pears" :: Nil))

  // List of Integers
  val nums2 = 1 :: (2 :: (3 :: (4 :: Nil)))

  // Empty List.
  val empty2 = Nil

  // Two dimensional list
  val dim2 = (1 :: (0 :: (0 :: Nil))) ::
    (0 :: (1 :: (0 :: Nil))) ::
    (0 :: (0 :: (1 :: Nil))) :: Nil

  def main(args: Array[String]) {
    val fruit = "apples" :: ("oranges" :: ("pears" :: Nil))
    val nums = Nil

    println("Head of fruit : " + fruit.head)
    println("Tail of fruit : " + fruit.tail)
    println("Check if fruit is empty : " + fruit.isEmpty)
    println("Check if nums is empty : " + nums.isEmpty)

    concatenatingLists

    println(List(1, 2, 3).zip(List("a", "b", "c")))

    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(numbers.partition(_ % 2 == 0))

    println(numbers.find((i: Int) => i > 5)) //返回集合中第一个匹配谓词函数的元素。
    println(numbers.drop(5))

    println(numbers.foldLeft(0)((m: Int, n: Int) => m + n))
    println(List(List(1, 2), List(3, 4)).flatten)

    val nestedNumbers = List(List(1, 2), List(3, 4))
    println(nestedNumbers.flatMap(x => x.map(_ * 2))) //flatMap是一种常用的组合子，结合映射[mapping]和扁平化[flattening]。 flatMap需要一个处理嵌套列表的函数，然后将结果串连起来。

    def timesTwo(i: Int): Int = i * 2
    println(ourMap(numbers, timesTwo(_)))

    //foreach很像map，但没有返回值。foreach仅用于有副作用[side-effects]的函数。
    numbers.foreach((i: Int) => i * 2)

    val extensions = Map("steve" -> 100, "bob" -> 101, "joe" -> 201)
    println(extensions.filter((namePhone: (String, Int)) => namePhone._2 < 200)) //._2标示第二个参数
    println(
      extensions.filter({ case (name, extension) => extension < 200 })
    )

    val m3 = Map((1, 100), (2, 200))
    for (e <- m3) println(e._1 + ": " + e._2)
    println(m3 filter (e => e._1 > 1))
    println(m3 filterKeys (_ > 1))
    println(m3.map(e => (e._1 * 10, e._2)))
    println(m3 map (e => e._2))

    println((1 to 5).map(2 *))
    println(1 to 5 by 2)
  }

  def ourMap(numbers: List[Int], fn: Int => Int): List[Int] = {
    numbers.foldRight(List[Int]()) { (x: Int, xs: List[Int]) =>
      fn(x) :: xs
    }
  }

  def concatenatingLists: Unit = {
    val fruit1 = "apples" :: ("oranges" :: ("pears" :: Nil))
    val fruit2 = "mangoes" :: ("banana" :: Nil)

    // use two or more lists with ::: operator
    var fruit = fruit1 ::: fruit2
    println("fruit1 ::: fruit2 : " + fruit)

    // use two lists with Set.:::() method
    fruit = fruit1.:::(fruit2)
    println("fruit1.:::(fruit2) : " + fruit)

    // pass two or more lists as arguments
    fruit = List.concat(fruit1, fruit2)
    println("List.concat(fruit1, fruit2) : " + fruit)
  }

}
