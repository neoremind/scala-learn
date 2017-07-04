import PartialApplicationFuncTest.Email

/**
  * @author zhangxu
  */
object PatternMatchingTest {

  def main(args: Array[String]) {
    println(matchTest("two"))
    println(matchTest("test"))
    println(matchTest(1))
    println(matchTest(2))
    println(matchTest(Email("title here", "", "", "")))

    val wordFrequencies = ("habitual", 6) :: ("and", 56) :: ("consuetudinary", 2) ::
      ("additionally", 27) :: ("homely", 5) :: ("society", 13) :: Nil
    println(wordsWithoutOutliersUgly(wordFrequencies))
    println(wordsWithoutOutliersGraceful(wordFrequencies))

    //////////

    def gameResults(): Seq[(String, Int)] =
      ("Daniel", 3500) :: ("Melissa", 13000) :: ("John", 7000) :: Nil

    def hallOfFame = for {
      result <- gameResults()
      (name, score) = result
      if score > 5000
    } yield name

    println(hallOfFame)

    ///////////

    case class Person(name: String, age: Int)
    def xu(): Person = Person("xu", 30)

    val Person(_, age) = xu()
    println(s"He is $age years old!")

    type Person2 = (String, Int)
    def xu2(): Person2 = ("xu", 40)

    val (name1, age1) = xu2()
    println(s"$name1 is $age1 years old!")
  }

  /**
    * re-bind the matched value with another name for `email`
    **/
  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
    case c@Email(_, _, _, _) => "Email: subject = %s".format(c.subject)
    case _ => "many"
  }

  /**
    * 丑陋的写法
    */
  def wordsWithoutOutliersUgly(wordFrequencies: Seq[(String, Int)]): Seq[String] =
    wordFrequencies.filter(wf => wf._2 > 3 && wf._2 < 25).map(_._1)

  /**
    * 模式匹配形式的匿名函数， 它是由一系列模式匹配样例组成的，正如模式匹配表达式那样，不过没有 match
    *
    * `_`是一个placeholder syntax
    */
  def wordsWithoutOutliersGraceful(wordFrequencies: Seq[(String, Int)]): Seq[String] =
    wordFrequencies.filter { case (_, f) => f > 3 && f < 25 } map { case (w, _) => w }

  /**
    * 其他例子
    */
  val predicate: (String, Int) => Boolean = {
    case (_, f) => f > 3 && f < 25
  }
  val transformFn: (String, Int) => String = {
    case (w, _) => w
  }
}
