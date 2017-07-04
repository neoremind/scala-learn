package programmingpattern.p10

/**
  * 替代访问者模式
  *
  * 为什么有CollectionUtils？因为为Collection添加新的实现容易，直接继承即可，但是添加新的操作困难。
  *
  * 访问者模式就是让添加新的操作容易，这是在OO的世界里面。
  *
  * scala是一门混血语言，min-in inheritance，trait，隐式转换是替代访问者模式的利器。
  */
object VisitorTest extends App {

  trait Person {
    def fullName: String

    def firstName: String

    def lastName: String

    def houseNum: Int

    def street: String
  }

  class SimplePerson(val firstName: String, val lastName: String,
                     val houseNum: Int, val street: String) extends Person {
    def fullName = firstName + " " + lastName
  }

  class ComplexPerson(name: Name, address: Address) extends Person {
    def fullName = name.firstName + " " + name.lastName

    def firstName = name.firstName

    def lastName = name.lastName

    def houseNum = address.houseNum

    def street = address.street
  }

  case class Address(houseNum: Int, street: String)

  case class Name(firstName: String, lastName: String)

  implicit class ExtendedPerson(person: Person) {
    def fullAddress = person.houseNum + " " + person.street
  }

  // 扩展simplePerson和complexPerson让他可以打印fullAddress，即使在他们中没有这个方法的定义
  val simplePerson = new SimplePerson("Mike", "Linn", 123, "No. #3")
  println(simplePerson.fullAddress)

  val complexPerson = new ComplexPerson(Name("Mike", "Linn"), Address(123, "No. #3"))
  println(complexPerson.fullAddress)

}
