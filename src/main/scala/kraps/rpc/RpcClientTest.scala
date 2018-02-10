package kraps.rpc

import org.apache.spark.SparkConf

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by xu.zhang on 7/23/17.
  */
object RpcClientTest {

  def main(args: Array[String]): Unit = {
    val config = RpcEnvConfig(new SparkConf(), HelloEndpoint.SYSTEM_NAME, "localhost", 52345, new SecurityManager, clientMode = true)
    val rpcEnvFactory: RpcEnvFactory = new NettyRpcEnvFactory
    val rpcEnv: RpcEnv = rpcEnvFactory.create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(HelloEndpoint.SYSTEM_NAME, RpcAddress("localhost", 52345), HelloEndpoint.ENDPOINT_NAME)
    val future: Future[String] = endPointRef.ask[String](SayHello("abc"))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => e.printStackTrace
    }
    Await.result(future, Duration.apply("30s"))
  }
}
