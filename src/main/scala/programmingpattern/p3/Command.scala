package programmingpattern.p3

/**
  * 替换命令模式
  */
class CashRegister(var total: Int) {
  def addCash(toAdd: Int) {
    total += toAdd
  }
}

object Register {
  def makePurchase(register: CashRegister, amount: Int) = {
    () => {
      println("Purchase in amount: " + amount)
      register.addCash(amount)
    }
  }

  var purchases: Vector[() => Unit] = Vector()

  def executePurchase(purchase: () => Unit) = {
    purchases = purchases :+ purchase
    purchase()
  }
}

object Main extends App {
  val register = new CashRegister(0)

  //返回函数，也就是命令本身
  val purchaseOne = Register.makePurchase(register, 100)
  val purchaseTwo = Register.makePurchase(register, 50)

  //执行命令
  Register.executePurchase(purchaseOne)
  Register.executePurchase(purchaseTwo)

  //计算总额
  println(register.total)
}
