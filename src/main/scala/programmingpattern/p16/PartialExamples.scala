package programmingpattern.p16

/**
  * 部分应用函数
  *
  * Created by xu.zhang on 7/2/17.
  */
object PartialExamples extends App {

  def addTwoInts(intOne: Int, intTwo: Int) = intOne + intTwo

  val addFortyTwo = addTwoInts(42, _: Int)

  println(addFortyTwo(100))

  def taxForState(amount: Double, state: Symbol) = state match {
    // Simple tax logic, for example only!
    case ('NY) => amount * 0.0645
    case ('PA) => amount * 0.045
    // Rest of states...
  }

  val nyTax = taxForState(_: Double, 'NY)
  val paTax = taxForState(_: Double, 'PA)
  val matchErrorTax = taxForState(_: Double, 'MATCHERROR)

  println(nyTax(100))
  println(paTax(100))
  println(matchErrorTax(100))
}
