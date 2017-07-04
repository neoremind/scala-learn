import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/**
  * A Scala 'Future' example from the Scala Cookbook.
  *
  * @see http://shop.oreilly.com/product/0636920026914.do
  */
object FutureExample extends App {

  // not too exciting, the result will always be 42. but more importantly, when?
  println("1 - starting calculation ...")
  val f = Future {
    sleep(Random.nextInt(500))
    42
  }

  println("2- before onComplete")
  f.onComplete {
    case scala.util.Success(value) => println(s"Got the callback, meaning = $value")
    case scala.util.Failure(e) => e.printStackTrace
  }

  f onSuccess {
    case result => println(s"Success: $result")
  }
  f onFailure {
    case t => println(s"Exception: ${t.getMessage}")
  }

  // do the rest of your work
  println("A ...");
  sleep(100)
  println("B ...");
  sleep(100)
  println("C ...");
  sleep(100)
  println("D ...");
  sleep(100)
  println("E ...");
  sleep(100)
  println("F ...");
  sleep(100)

  // keep the jvm alive (may be needed depending on how you run the example)
  sleep(2000)

  def sleep(duration: Long) {
    Thread.sleep(duration)
  }

  ///////////////
  object Cloud {
    def runAlgorithm(i: Int): Future[Int] = Future {
      sleep(Random.nextInt(3000))
      val result = i + 10
      println(s"returning result from cloud: $result")
      result
    }
  }

  println("starting futures")
  val result1 = Cloud.runAlgorithm(10)
  val result2 = Cloud.runAlgorithm(20)
  val result3 = Cloud.runAlgorithm(30)

  println("before for-comprehension")
  val result = for {
    r1 <- result1
    r2 <- result2
    r3 <- result3
  } yield (r1 + r2 + r3)

  println("before onSuccess")
  result onSuccess {
    case result => println(s"total = $result")
  }

  println("before sleep at the end")
  sleep(5000)  // important: keep the jvm alive
}
