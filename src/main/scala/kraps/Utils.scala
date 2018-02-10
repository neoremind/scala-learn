package kraps.spark

import java.io.IOException
import java.net.BindException

import org.apache.spark.{SparkConf, SparkException}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class MyRdd(name: String) {

  var callSite: CallSite = _

  def flatMap() = {
    callSite = Utils.getCallSite()
  }

  def getCallSite(): CallSite = {
    callSite
  }
}

/**
  * 从spark utils里面copy过来的
  */
object Utils {

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    // A regular expression to match classes of the internal Spark API's
    // that we want to skip when finding the call site of a method.
    val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
    val SPARK_SQL_CLASS_REGEX = """^org\.apache\.spark\.sql.*""".r
    val MYTEST_CLASS_REGEX = """^kraps\.spark.*""".r
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SPARK_SQL_CLASS_REGEX.findFirstIn(className).isDefined ||
      MYTEST_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass
  }

  /**
    * When called inside a class in the spark package, returns the name of the user code class
    * (outside the spark package) that called into Spark, as well as which Spark method they called.
    * This is used, for example, to tell users where in their code each RDD got created.
    *
    * @param skipClass Function that is used to exclude non-user-code classes.
    */
  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction): CallSite = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    var callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      // When running under some profilers, the current stack trace might contain some bogus
      // frames. This is intended to ensure that we don't crash in these situations by
      // ignoring any frames that we can't examine.
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            firstUserLine = ste.getLineNumber
            firstUserFile = ste.getFileName
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname
  }

  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      //      case e: MultiException =>
      //        e.getThrowables.asScala.exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             conf: SparkConf,
                             serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = 10
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        println(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${e.getMessage}: Service$serviceString failed after " +
              s"$maxRetries retries! Consider explicitly setting the appropriate port for the " +
              s"service$serviceString (for example spark.ui.port for SparkUI) to an available " +
              "port or increasing spark.port.maxRetries."
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          println(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        println("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        println("Exception encountered", e)
        throw new IOException(e)
    }
  }
}

/** CallSite represents a place in user code. It can have a short and a long form. */
case class CallSite(shortForm: String, longForm: String)

object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
  val empty = CallSite("", "")
}
