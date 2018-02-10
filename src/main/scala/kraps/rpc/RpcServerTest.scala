package kraps.rpc

import org.apache.spark.{HeartbeatResponse, SparkConf}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

case class SayHello(msg: String)

/**
  * Created by xu.zhang on 7/23/17.
  */
object RpcServerTest {

  def main(args: Array[String]): Unit = {
    val config = RpcEnvConfig(new SparkConf(), HelloEndpoint.SYSTEM_NAME, "localhost", 52345, new SecurityManager, clientMode = false)
    val rpcEnvFactory: RpcEnvFactory = new NettyRpcEnvFactory
    val rpcEnv: RpcEnv = rpcEnvFactory.create(config)
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint(HelloEndpoint.ENDPOINT_NAME, helloEndpoint)
    val f = Future {
      val endPointRef: RpcEndpointRef = rpcEnv.endpointRef(helloEndpoint)
      val future: Future[String] = endPointRef.ask[String](SayHello("abc"))
      future.onComplete {
        case scala.util.Success(value) => println(s"client got result => $value")
        case scala.util.Failure(e) => e.printStackTrace
      }
    }
    Await.result(f, Duration.apply("240s"))
    println("waiting to be called...")
    rpcEnv.awaitTermination()
  }

}

class HelloEndpoint(realRpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // Messages sent and received locally
    case SayHello(msg) => {
      println(s"receive $msg")
      context.reply(msg.toUpperCase)
    }
  }

  override def onStop(): Unit = {
    println("stop hello...")
  }

  /**
    * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
    */
  override val rpcEnv: RpcEnv = realRpcEnv
}

object HelloEndpoint {
  val SYSTEM_NAME = "hello"
  val ENDPOINT_NAME = "my-hello"
}