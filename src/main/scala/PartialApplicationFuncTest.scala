/**
  * Scala 允许部分应用一个函数。 调用一个函数时，不是把函数需要的所有参数都传递给它，
  * 而是仅仅传递一部分，其他参数留空； 这样会生成一个新的函数，其参数列表由那些被留空的参数组成。
  * （不要把这个概念和偏函数混淆）
  *
  * [[https://windor.gitbooks.io/beginners-guide-to-scala/content/chp11-currying-and-partially-applied-functions.html]]
  */
object PartialApplicationFuncTest {

  def main(args: Array[String]): Unit = {
    val email1 = Email("hello", "hello, my friend", "ap@gmail.com", "haha@gmail.com")
    println(minimumSize(20, email1))
    println(minimumSize(1, email1))
  }

  case class Email(
                    subject: String,
                    text: String,
                    sender: String,
                    recipient: String)

  type EmailFilter = Email => Boolean

  /** 谓词函数定义了别名 IntPairPred ，该函数接受一对整数（值 n 和邮件内容长度），检查邮件长度对于 n 是否 OK。 */
  type IntPairPred = (Int, Int) => Boolean

  def sizeConstraint(pred: IntPairPred, n: Int, email: Email) =
    pred(email.text.size, n)

  val gt: IntPairPred = _ > _
  val ge: IntPairPred = _ >= _
  val lt: IntPairPred = _ < _
  val le: IntPairPred = _ <= _
  val eq: IntPairPred = _ == _

  /** 对所有没有传入值的参数，必须使用占位符 _ ，还需要指定这些参数的类型 */
  val minimumSize: (Int, Email) => Boolean = sizeConstraint(ge, _: Int, _: Email)
  val maximumSize: (Int, Email) => Boolean = sizeConstraint(le, _: Int, _: Email)

  val constr20: (IntPairPred, Email) => Boolean =
    sizeConstraint(_: IntPairPred, 20, _: Email)

  val constr30: (IntPairPred, Email) => Boolean =
    sizeConstraint(_: IntPairPred, 30, _: Email)

  val sizeConstraintFn: (IntPairPred, Int, Email) => Boolean = sizeConstraint _

}
