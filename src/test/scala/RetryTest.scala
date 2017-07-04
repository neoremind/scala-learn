import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.concurrent.duration._

/**
  * Retry test
  *
  * @author xu.zhang
  */
class RetryTest extends FlatSpec {

  implicit val retryStrategy =
    RetryStrategy.fixedBackOff(retryDuration = 1.second, maxAttempts = 2)

  "A `Retry` " should "return `Success` in case of a successful operation" in {
    Retry(1 / 1) should be(Success(1))
  }

  "A `Retry` " should "be used in for comprehensions" in {
    val result = for {
      x <- Retry(1 / 1)
      y <- Retry(1 / 1)
    } yield x + y

    result should be(Success(2))
  }

  "A `Retry` " should "recover in case of a failure `Failure` " in {
    val result =
      Retry(1 / 0) recover {
        case _ => -1
      }
    result should be(Success(-1))
  }

  "A `Retry.get` " should " return the computed value" in {
    val result = Retry(1 / 1).get
    result should be(1)
  }

  "A `Retry.get` " should " throw an exception in case of a failure" in {
    try {
      Retry(1 / 0).get
      fail("should not get here")
    } catch {
      case e: ArithmeticException => ()
    }
  }

  "A `Retry.getOrElse` " should " should return a value even if an exception is thrown in the execution" in {
    val result = Retry(1 / 0).getOrElse(1)
    result should be(1)
  }

}
