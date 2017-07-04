package programmingpattern.p17

/**
  * 记忆模式，简单缓存
  *
  * Created by xu.zhang on 7/2/17.
  */
object Memoization extends App {

  def expensiveLookup(id: Int) = {
    Thread.sleep(1000)
    println(s"Doing expensive lookup for $id")
    Map(42 -> "foo", 12 -> "bar", 1 -> "baz").get(id)
  }

  def memoizeExpensiveLookup() = {
    var cache = Map[Int, Option[String]]()
    (id: Int) =>
      cache.get(id) match {
        case Some(result: Option[String]) => result
        case None => {
          val result = expensiveLookup(id)
          cache += id -> result
          result
        }
      }
  }

  val memorizedExpensiveLookup = memoizeExpensiveLookup

  println(memorizedExpensiveLookup(42))
  println(memorizedExpensiveLookup(42))
  println(memorizedExpensiveLookup(12))
  println(memorizedExpensiveLookup(99999))
  println(memorizedExpensiveLookup(12))

}
