/**
 *
 * http://www.zhihu.com/question/26707124
 *
 * @author zhangxu
 */
object HelloWorld {

  def main(args: Array[String]) {
    print("hello world!")
    println("12345".map(_+ 1))
    (1 to 9).filter(_ % 2 == 0).map(10 * _).foreach(println _)
  }

}