package programmingpattern.p8

/**
  * 替代空对象
  */
object Examples extends App {

  case class Person(firstName: String = "John", lastName: String = "Doe")

  val nullPerson = Person()

  def fetchPerson(people: Map[Int, Person], id: Int) =
    people.getOrElse(id, nullPerson)

  val joe = Person("Joe", "Smith")
  val jack = Person("Jack", "Brown")
  val somePeople = Map(1 -> joe, 2 -> jack)

  println(fetchPerson(somePeople, 1))
  println(fetchPerson(somePeople, 2))
  println(fetchPerson(somePeople, 3))

  ///////////////

  def vecFoo = Vector("foo")

  def someFoo = Some("foo")

  def someBar = Some("bar")

  def aNone = None

  def buildPerson(firstNameOption: Option[String], lastNameOption: Option[String]) =
    (for (
      firstName <- firstNameOption;
      lastName <- lastNameOption)
      yield Person(firstName, lastName)).getOrElse(Person("John", "Doe"))

  println(buildPerson(Some("Mike"), Some("Linn")))
  println(buildPerson(Some("Mike"), None))
}
