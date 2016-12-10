package spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
  * Command to launch:
  * `./bin/spark-submit --class spark.StreamingWordCount myjar/scala-learn_2.11-1.0.jar 10`
  *
  * http://spark.apache.org/docs/latest/streaming-programming-guide.html
  *
  * Created by helechen on 2016/12/10.
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(args(0).toInt))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}
