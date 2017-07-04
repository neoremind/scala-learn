package programmingpattern.p5

/**
  * 替代迭代器模式
  *
  * Created by xu.zhang on 7/3/17.
  */
object HigherOrderFunctions extends App {

  def sumSequence(sequence: Seq[Int]) =
    if (sequence.isEmpty) 0 else sequence.reduce((acc, curr) => acc + curr)

  println(sumSequence(Seq(1, 2, 3, 4)))

  def prependHello(names: Seq[String]) =
    names.map((name) => "Hello, " + name)

  println(Seq("jack", "neo"))

  val isVowel: Set[Char] = Set('a', 'e', 'i', 'o', 'u') //很特别的用法

  println(isVowel('a'))
  println(isVowel('x'))

  def vowelsInWord(word: String) = word.filter(isVowel).toSet

  println(vowelsInWord("mklw"))
  println(vowelsInWord("ajok"))

}
