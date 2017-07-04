package programmingpattern.p7

/**
  * 替换策略模式
  */
object PeopleExample extends App {

  case class Person(firstName: Option[String],
                    middleName: Option[String],
                    lastName: Option[String])

  def isFirstNameValid(person: Person) = person.firstName.isDefined

  def isFullNameValid(person: Person) = person match {
    case Person(firstName, middleName, lastName) =>
      firstName.isDefined && middleName.isDefined && lastName.isDefined
  }

  /**
    * 返回的是一个函数，并且封装了可变性，每次调用都会修改内部的validPeople集合
    */
  def personCollector(isValid: (Person) => Boolean) = {
    var validPeople = Vector[Person]()
    (person: Person) => {
      if (isValid(person)) validPeople = validPeople :+ person
      validPeople
    }
  }

  val p1 = Person(Some("John"), Some("Quincy"), Some("Adams"))
  val p2 = Person(Some("Mike"), None, Some("Linn"))
  val p3 = Person(None, None, None)

  val singleNameValidCollector = personCollector(isFirstNameValid)
  println(singleNameValidCollector(p1)) //Vector(Person(Some(John),Some(Quincy),Some(Adams)))
  println(singleNameValidCollector(p2)) //Vector(Person(Some(John),Some(Quincy),Some(Adams)), Person(Some(Mike),None,Some(Linn)))
  println(singleNameValidCollector(p3)) //Vector(Person(Some(John),Some(Quincy),Some(Adams)), Person(Some(Mike),None,Some(Linn)))

  val fullNameValidCollector = personCollector(isFullNameValid)
  println(fullNameValidCollector(p1))
  println(fullNameValidCollector(p2))
  println(fullNameValidCollector(p3))
}
