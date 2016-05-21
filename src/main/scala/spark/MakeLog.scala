package spark

import java.io.{File, PrintWriter}

import scala.util.Random

/**
 * 在Spark上实验的模仿网盟的pb log入OLAP Engine的日志
 *
 * 一共6列，分别为：
 * userid，planid，groupid，show，clk，cost
 *
 * @author zhangxu
 */
object MakeLog {

  def main(args: Array[String]) {
    testWriteFile()

    testMapReduce()
  }

  def testWriteFile(): Unit = {
    val writer = new PrintWriter(new File("/Users/baidu/reportlogs.txt"))
    for (i <- 1 to 10) {
      val logs = genShuffledLogs(1000 * 1000)
      logs.foreach(writer.println(_))
    }
    writer.close()
  }

  def testMapReduce(): Unit = {
    var shuffledLogs = List[Log]()
    for (i <- 1 to 2) {
      shuffledLogs = shuffledLogs ++ genShuffledLogs(100 * 100)
    }
    val start = System.currentTimeMillis()
    val res = mapReduce(shuffledLogs)
    println("using " + (System.currentTimeMillis() - start) + "ms")
    println(res.size)
    println(res.get(100))
  }

  def mapReduce(shuffledLogs: List[Log]): Map[Int, Log] = {
    shuffledLogs.groupBy(_.groupId).mapValues(l => l.reduce(_.merge(_)))
  }

  def genShuffledLogs(groupNum: Int): IndexedSeq[Log] = {
    val logs = (1 to groupNum).map {
      i => {
        new Log(i / 100 + 1, i / 10 + 1, i, showRandom, clkRandom, costRandom)
      }
    }
    Random.shuffle(logs)
  }

  def showRandom = random(100, 1000)

  def clkRandom = random(0, 100)

  def costRandom = random(0, 100)

  def random(offset: Int, span: Int): Int = {
    val rnd = new Random
    offset + rnd.nextInt(span)
  }
}

case class Log(userId: Int, planId: Int, groupId: Int, var show: Int, var clk: Int, var cost: Int) {

  def merge(log: Log): Log = {
    this.show += log.show
    this.clk += log.clk
    this.cost += log.cost
    this
  }

  override def toString: String = "%d\t%d\t%d\t%d\t%d\t%d".format(userId, planId, groupId, show, clk, cost)
}



