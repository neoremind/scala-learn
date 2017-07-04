package programmingpattern.p1

/**
  * 替代函数式接口
  *
  * OO里面的函数式接口，例如run，execute，apply，perform等，只有一个方法，执行明确的一个行为
  *
  * 函数式接口让我们将对象作为函数调用，传递给程序的参数由名词变为动词，颠覆OO世界观。
  *
  * 从此函数、方法、动词不再是"二等公民"
  *
  * 函数式语言里面，函数都是high order的，他们可以作为返回值也可以作为入参。
  *
  * 下面的例子分别是匿名函数和具名函数
  */
object PersonExample extends App {
  (int1: Int, int2: Int) => int1 + int2

  case class Person(firstName: String, lastName: String)

  val p1 = Person("Michael", "Bevilacqua")
  val p2 = Person("Pedro", "Vasquez")
  val p3 = Person("Robert", "Aarons")

  val people = Vector(p3, p2, p1)

  println(people)
  println(people.sortWith((p1, p2) => p1.firstName < p2.firstName))

}

object PersonExpanded extends App {

  case class Person(firstName: String, middleName: String, lastName: String)

  val p1 = Person("Aaron", "Jeffrey", "Smith")
  val p2 = Person("Aaron", "Bailey", "Zanthar")
  val p3 = Person("Brian", "Adams", "Smith")
  val people = Vector(p1, p2, p3)

  def complicatedSort(p1: Person, p2: Person) =
    if (p1.firstName != p2.firstName)
      p1.firstName < p2.firstName
    else if (p1.lastName != p2.lastName)
      p1.lastName < p2.lastName
    else
      p1.middleName < p2.middleName

  println(people)
  println(people.sortWith(complicatedSort))

}
