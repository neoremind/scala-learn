package programmingpattern.p16

import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.collection.mutable.WrappedArray

/**
  * 函数生成器，Map的key选择器
  */
object Selector extends App {

  //TODO: change this all to use functions instead of methods.

  def selector(path: Symbol*): (Map[Symbol, Any] => Option[Any]) = {

    if (path.size <= 0) throw new IllegalArgumentException("path must not be empty")

    @tailrec
    def selectorHelper(path: Seq[Symbol], ds: Map[Symbol, Any]): Option[Any] =
      if (path.size == 1) {
        ds.get(path(0))
      } else {
        val currentPiece = ds.get(path.head)
        currentPiece match {
          case Some(currentMap: Map[Symbol, Any]) =>
            selectorHelper(path.tail, currentMap)
          case None => None
          case _ => None
        }
      }

    (ds: Map[Symbol, Any]) => selectorHelper(path.toSeq, ds)
  }

  val simplePerson = Map('name -> "Michael")
  val name = selector('name)
  println(name(simplePerson))

  val complexPerson = Map('name -> Map('first -> "Michael", 'last -> "Linn"))
  val name2 = selector('name, 'first)
  println(name2(complexPerson))

}
