/**
 *
 * http://www.zhihu.com/question/26707124
 *
 * @author zhangxu
 */
object HelloWorld {

  def main(args: Array[String]) {
    print("hello world!")
    println("12345".map(_ + 1))
    (1 to 9).filter(_ % 2 == 0).map(10 * _).foreach(println _)
    val x = "1*1".getSizeBitMask
    println(x)
    println(Int.MaxValue.toLong + Int.MaxValue.toLong)

    val width = 2147483648600L & 0xFFFFFFFFL
    val height = 2147483648600L >> 32
    println(width + "\t" + height)
    println("300*250".getSizeBitMask)
  }

  implicit class get(s: String) {
    def getSizeBitMask = {
      try {
        val widthAndHeight = s.split("\\*")
        widthAndHeight(1).toLong << 32 | widthAndHeight(0).toLong
      } catch {
        case e: Exception => {
          e.printStackTrace()
          0L
        }
      }
    }
  }

}