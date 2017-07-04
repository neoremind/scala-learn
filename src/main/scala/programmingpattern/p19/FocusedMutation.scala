package programmingpattern.p19

import scala.util.Random
import scala.collection.immutable.Stream
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import java.util.concurrent.TimeUnit

import programmingpattern.p16.CompositionExamples.Email

import scala.collection._

/**
  * 集中可变性.封装可变性，进而提高处理性能，不可变虽然好处多，但是效率不如可变。
  */
object Examples extends App {

  // copy方法和带名参数
  val mail = Email("subject", "", "", "")
  println(mail)
  val mailCopy = mail.copy(subject = "haha", text = "hello")
  println(mailCopy)

  ///////////////////

  case class Purchase(storeNumber: Int, customerNumber: Int, itemNumber: Int)

  val r = new Random

  def makeTestPurchase = Purchase(r.nextInt(100), r.nextInt(1000), r.nextInt(500))

  // 惰性序列
  def infiniteTestPurchases: Stream[Purchase] =
    makeTestPurchase #:: infiniteTestPurchases

  for (purchase <- infiniteTestPurchases take 5) println(purchase)

  val oneHundredThousand = 100000
  val fiveHundredThousand = 500000

  def immutableSequenceEventProcessing(count: Int) = {
    val testPurchases = infiniteTestPurchases.take(count)
    var mapOfPurchases = immutable.Map[Int, List[Purchase]]()

    for (purchase <- testPurchases)
      mapOfPurchases.get(purchase.storeNumber) match {
        case None => mapOfPurchases =
          mapOfPurchases + (purchase.storeNumber -> List(purchase))
        case Some(existing: List[Purchase]) => mapOfPurchases =
          mapOfPurchases + (purchase.storeNumber -> (purchase :: existing))
      }
  }

  timeRuns(immutableSequenceEventProcessing(fiveHundredThousand), 5)

  def mutableSequenceEventProcessing(count: Int) = {
    val testPurchases = infiniteTestPurchases.take(count)
    val mapOfPurchases = mutable.Map[Int, List[Purchase]]()

    for (purchase <- testPurchases)
      mapOfPurchases.get(purchase.storeNumber) match {
        case None => mapOfPurchases.put(purchase.storeNumber, List(purchase))
        case Some(existing: List[Purchase]) =>
          mapOfPurchases.put(purchase.storeNumber, (purchase :: existing))
      }

    mapOfPurchases.toMap
  }

  timeRuns(mutableSequenceEventProcessing(fiveHundredThousand), 5)

  ////////////////////////////
  // 添加元素到索引序列

  def time[R](block: => R): R = {
    val start = System.nanoTime
    val result = block
    val end = System.nanoTime
    val elapsedTimeMs = (end - start) * 0.000001
    println("Elapsed time: %.3f msecs".format(elapsedTimeMs))
    result
  }

  def timeRuns[R](block: => R, count: Int) =
    for (_ <- Range(0, count)) time {
      block
    }

  val oneMillion = 1000000

  timeRuns(testImmutable(oneMillion), 5)

  //////////

  def testImmutable(count: Int): IndexedSeq[Int] = {
    var v = Vector[Int]()
    for (c <- Range(0, count))
      v = v :+ c
    v
  }

  println(testImmutable(5))

  def testMutable(count: Int): IndexedSeq[Int] = {
    val s = ArrayBuffer[Int](count)
    for (c <- Range(0, count))
      s.append(c)
    s.toIndexedSeq
  }

  println(testMutable(5))

  def testMutableNoConversion(count: Int): IndexedSeq[Int] = {
    val s = ArrayBuffer[Int](count)
    for (c <- Range(0, count))
      s.append(c)
    s
  }

  println(testMutableNoConversion(5))
}