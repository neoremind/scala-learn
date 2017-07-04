import scala.util.Try

/**
  * 偏函数是只对函数定义域的一个子集进行定义的函数
  */
object PartialFunctionTest extends App {

  val numbers = Seq(("eight", 8), ("four", 4), ("two", 2), ("six", 6))

  /**
    * 必须放在`println(numbers.collect(pf))`前面定义好
    */
  val pf: PartialFunction[(String, Int), String] = {
    case (word, freq) if freq > 3 && freq < 25 => word
  }

  /**
    * 可以使用 val handler = fooHandler orElse barHandler orElse bazHandler
    */
  val pfWithOrElse: PartialFunction[(String, Int), String] = pf.orElse {
    case (word, freq) => "not valid"
  }

  println(Try(numbers.map(pf))) // will throw a MatchError: (two,2) (of class scala.Tuple2)
  println(numbers.collect(pf))
  println(numbers.collect(pfWithOrElse))

  //等价于
  val pf2 = new PartialFunction[(String, Int), String] {
    def apply(wordFrequency: (String, Int)) = wordFrequency match {
      case (word, freq) if freq > 3 && freq < 25 => word
    }

    def isDefinedAt(wordFrequency: (String, Int)) = wordFrequency match {
      case (word, freq) if freq > 3 && freq < 25 => true
      case _ => false
    }
  }
}
