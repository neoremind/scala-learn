/**
 * @author zhangxu
 */
class Apple(var name: String) {
}

object GetFruit {

  implicit class get(s: String) {
    def getApple = new Apple(s)
  }

  def main(args: Array[String]) {
    val apple: Apple = "hello".getApple
    apple.name = "123"
    println(apple.name)
  }
}
