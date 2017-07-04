package programmingpattern.p14

/**
  * filter-map-reduce
  *
  * Created by xu.zhang on 6/30/17.
  */
object Discount extends App {

  def calculateDiscount(prices: Seq[Double]): Double = {
    prices filter (price => price >= 20.0) map
      (price => price * 0.10) reduce
      ((total, price) => total + price)
  }

  def calculateDiscountNamedFn(prices: Seq[Double]): Double = {
    def isGreaterThan20(price: Double) = price >= 20.0

    def tenPercent(price: Double) = price * 0.10

    def sumPrices(total: Double, price: Double) = total + price

    prices filter isGreaterThan20 map tenPercent reduce sumPrices
  }

  println(calculateDiscount(Seq(20.2, 3.4, 50.6)))
  println(calculateDiscountNamedFn(Seq(20.2, 3.4, 50.6)))
}
