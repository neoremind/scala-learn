package spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Integrate hive into spark unit test case
  *
  * @author xu.zhang
  */
class WordCountSparkTest extends FlatSpec with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  before {
    //assert("127.0.0.1".equals(System.getenv("SPARK_LOCAL_IP")))

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "Shakespeare most famous quote" should "be counted" in {
    val lines = Array("To be or not to be.", "That is the question.")

    val stopWords = Set("the")

    val wordCounts = WordCount.count(sc.parallelize(lines), stopWords).collect()

    wordCounts should equal(Array(
      WordCount("be", 2),
      WordCount("is", 1),
      WordCount("not", 1),
      WordCount("or", 1),
      WordCount("question", 1),
      WordCount("that", 1),
      WordCount("to", 2)))
  }

}

case class WordCount(word: String, count: Int) extends Serializable

object WordCount {
  def count(lines: RDD[String], stopWords: Set[String]): RDD[WordCount] = {
    val words = lines.flatMap(_.split("\\s"))
      .map(s => StringUtils.strip(s, ".").toLowerCase)
      .filter(!stopWords.contains(_)).filter(!_.isEmpty)

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    val sortedWordCounts = wordCounts.sortBy(_.word)

    sortedWordCounts
  }
}
