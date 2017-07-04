package programmingpattern.p5

/**
  * 替代迭代器模式
  *
  * 序列推导
  *
  * 将一个序列转换为另外一个序列，同事可以做过滤
  */
object TheLambdaBarAndGrille extends App {

  case class Person(name: String, address: Address)

  case class Address(zip: Int)

  def generateGreetings(people: Seq[Person]) =
    for (Person(name, address) <- people if isCloseZip(address.zip))
      yield "Hello, %s, and welcome to the Lambda Bar And Grille!".format(name)

  def isCloseZip(zipCode: Int) = zipCode == 19123 || zipCode == 19103

  val people = Seq(Person("jack", Address(123)),
    Person("neo", Address(19123)),
    Person("hellen", Address(555)),
    Person("raj", Address(19103))
  )

  generateGreetings(people) foreach println

  def printGreetings(people: Seq[Person]) =
    for (Person(name, address) <- people if isCloseZip(address.zip))
      println("Hello, %s, and welcome to the Lambda Bar And Grille!".format(name))

  printGreetings(people)
}
