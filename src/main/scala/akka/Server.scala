package akka

import akka.actor.Actor
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props

case class AkkaMessage(message: Any)

case class Response(response: Any)

/**
 * 参考http://lxw1234.com/archives/2016/03/617.htm?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io
 *
 * author http://lxw1234.com
 */
class Server extends Actor {
  override def receive: Receive = {
    //接收到的消息类型为AkkaMessage，则在前面加上response_，返回给sender
    case msg: AkkaMessage => {
      println("服务端收到消息: " + msg.message)
      sender ! Response("response_" + msg.message)
    }
    case _ => println("服务端不支持的消息类型 .. ")
  }
}

object Server {
  //创建远程Actor:ServerSystem
  def main(args: Array[String]): Unit = {
    val serverSystem = ActorSystem("neoremind", ConfigFactory.parseString( """
      akka {
       actor {
          provider = "akka.remote.RemoteActorRefProvider"
        }
        remote {
          enabled-transports = ["akka.remote.netty.tcp"]
          netty.tcp {
            hostname = "127.0.0.1"
            port = 2555
          }
        }
      }
                                                                           """))

    serverSystem.actorOf(Props[Server], "server")

  }
}
