package programmingpattern.p16

/**
  * 函数生成器
  */
object DiscountBuilder extends App {

  def discount(percent: Double) = {
    if (percent < 0.0 || percent > 100.0)
      throw new IllegalArgumentException("Discounts must be between 0.0 and 100.0.")
    (originalPrice: Double) =>
      originalPrice - (originalPrice * percent * 0.01)
  }

  println(discount(50)(200))
  val twentyFivePercentOff = discount(25)
  println(twentyFivePercentOff(200))
  println(Vector(100.0, 25.0, 50.0, 25.0) map twentyFivePercentOff sum)

}
