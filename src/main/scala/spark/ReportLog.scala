package spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Command to launch:
  * `./bin/spark-submit --class spark.ReportLog     --master yarn     --deploy-mode cluster     --driver-memory 4g     --executor-memory 2g     --executor-cores 1      myjar/scala-learn_2.11-1.0.jar     /reportlog/reportlogs.raw.1000 /reportlog/reportlog.1000.res`
  */
object ReportLog {

  def main(args: Array[String]) {
    //    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    //    val conf = new SparkConf().setAppName("Simple Application")
    //    val sc = new SparkContext(conf)
    //    val logData = sc.textFile(logFile, 2).cache()
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val conf = new SparkConf().setAppName("ReportLog")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    //line.flatMap(_.split("\t")).map((_, 2)).reduceByKey(_ + _).collect().foreach(println)
    val res = lines.map(line => {
      val fields = line.split("\t")
      val userId = fields(0).toInt
      val planId = fields(1).toInt
      val groupId = fields(2).toInt
      val key = "%d_%d_%d".format(userId, planId, groupId)
      (key, new Log(userId, planId, groupId, fields(3).toInt, fields(4).toInt, fields(5).toInt))
    }).partitionBy(ShardPartitioner(10)).reduceByKey(_.merge(_)).map(_._2).saveAsTextFile(args(1))
  }

  case class ShardPartitioner(shardNum: Int) extends Partitioner {
    override def numPartitions: Int = 10

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      val groupId = k.split("_")(2).toInt
      // `k` is assumed to go continuously from 0 to elements-1.
      groupId % 10
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

}