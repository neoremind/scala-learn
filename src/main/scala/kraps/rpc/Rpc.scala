package kraps.rpc

import java.io.{ObjectInputStream, ObjectOutputStream, Serializable}
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, TimeoutException}
import javax.annotation.Nullable

import kraps.ThreadUtils
import kraps.spark.Utils
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.{SparkConf, SparkException}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Awaitable, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{DynamicVariable, Failure, Success}

trait RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv
}

class NettyRpcEnvFactory extends RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
    //new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    new JavaSerializerInstance(0, false, Thread.currentThread().getContextClassLoader)
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.host, config.securityManager)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

case class RpcEnvConfig(conf: SparkConf,
                        name: String,
                        host: String,
                        port: Int,
                        securityManager: SecurityManager,
                        clientMode: Boolean)

abstract class RpcEnv(conf: SparkConf) {

  import scala.concurrent.duration._

  val defaultLookupTimeout = new RpcTimeout(120.seconds, "dummy")

  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    */
  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  def address: RpcAddress

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
    */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`.
    * This is a blocking action.
    */
  def setupEndpointRef(systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  def awaitTermination(): Unit

  /**
    * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
    * creating it manually because different [[RpcEnv]] may have different formats.
    */
  def uriOf(systemName: String, address: RpcAddress, endpointName: String): String

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  def deserialize[T](deserializationAction: () => T): T

}

class RpcTimeoutException(message: String, cause: TimeoutException)
  extends TimeoutException(message) {
  initCause(cause)
}

class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
  extends Serializable {

  /** Amends the standard message of TimeoutException to include the description */
  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage + ". This timeout is controlled by " + timeoutProp, te)
  }

  /**
    * PartialFunction to match a TimeoutException and add the timeout description to the message
    *
    * @note This can be used in the recover callback of a Future to add to a TimeoutException
    *       Example:
    *       val timeout = new RpcTimeout(5 millis, "short timeout")
    *       Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
    */
  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  /**
    * Wait for the completed result and return it. If the result is not available within this
    * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
    *
    * @param  awaitable the `Awaitable` to be awaited
    * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
    *                             is still not ready
    */
  def awaitResult[T](awaitable: Awaitable[T]): T = {
    try {
      Await.result(awaitable, duration)
    } catch addMessageIfTimeout
  }
}

case class RpcAddress(host: String, port: Int) {

  def hostPort: String = host + ":" + port

  /** Returns a string in the form of "spark://host:port". */
  def toSparkURL: String = "spark://" + hostPort

  override def toString: String = hostPort
}


object RpcAddress {

  /** Return the [[RpcAddress]] represented by `uri`. */
  def fromURIString(uri: String): RpcAddress = {
    val uriObj = new java.net.URI(uri)
    RpcAddress(uriObj.getHost, uriObj.getPort)
  }
}

trait RpcEndpoint {

  /**
    * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
    */
  val rpcEnv: RpcEnv

  /**
    * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
    * called. And `self` will become `null` when `onStop` is called.
    *
    * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
    * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
    */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
    * Process messages from [[RpcEndpointRef.send]] or [[RpcCallContext.reply)]]. If receiving a
    * unmatched message, [[SparkException]] will be thrown and sent to `onError`.
    */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
    * Process messages from [[RpcEndpointRef.ask]]. If receiving a unmatched message,
    * [[SparkException]] will be thrown and sent to `onError`.
    */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
    * Invoked when any exception is thrown during handling messages.
    */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
    * Invoked when `remoteAddress` is connected to the current node.
    */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when `remoteAddress` is lost.
    */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when some network error happens in the connection between the current node and
    * `remoteAddress`.
    */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked before [[RpcEndpoint]] starts to handle any message.
    */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
    * use it to send or ask messages.
    */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
    * A convenient method to stop [[RpcEndpoint]].
    */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
  * A trait that requires RpcEnv thread-safely sending messages to it.
  *
  * Thread-safety means processing of one message happens before processing of the next message by
  * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
  * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
  * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
  *
  * However, there is no guarantee that the same thread will be executing the same
  * [[ThreadSafeRpcEndpoint]] for different messages.
  */
trait ThreadSafeRpcEndpoint extends RpcEndpoint

abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable {

  import scala.concurrent.duration._

  private[this] val maxRetries = 2
  private[this] val retryWaitMs = 2000
  private[this] val defaultAskTimeout = new RpcTimeout(30.seconds, "dummy")

  /**
    * return the address for the [[RpcEndpointRef]]
    */
  def address: RpcAddress

  def name: String

  /**
    * Sends a one-way asynchronous message. Fire-and-forget semantics.
    */
  def send(message: Any): Unit

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within the specified timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within a default timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
    * timeout, or throw a SparkException if this fails even after the default number of retries.
    * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
    * method retries, the message handling in the receiver side should be idempotent.
    *
    * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
    * loop of [[RpcEndpoint]].
    *
    * @param message the message to send
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
    * specified timeout, throw a SparkException if this fails even after the specified number of
    * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
    * retries, the message handling in the receiver side should be idempotent.
    *
    * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
    * loop of [[RpcEndpoint]].
    *
    * @param message the message to send
    * @param timeout the timeout duration
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askWithRetry[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = ask[T](message, timeout)
        val result = timeout.awaitResult(future)
        if (result == null) {
          throw new SparkException("RpcEndpoint returned null")
        }
        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          println(s"Error sending message [message = $message] in $attempts attempts", e)
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

}

class RpcEndpointNotFoundException(uri: String)
  extends SparkException(s"Cannot find endpoint: $uri")


trait RpcCallContext {

  /**
    * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
    * will be called.
    */
  def reply(response: Any): Unit

  /**
    * Report a failure to the sender.
    */
  def sendFailure(e: Throwable): Unit

  /**
    * The sender of this message.
    */
  def senderAddress: RpcAddress
}

abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }

}

class NettyRpcHandler(
                       dispatcher: Dispatcher,
                       nettyEnv: NettyRpcEnv,
                       streamManager: StreamManager) extends RpcHandler {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
                        client: TransportClient,
                        message: ByteBuffer,
                        callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
                        client: TransportClient,
                        message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      println("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}

class NettyRpcEndpointRef(
                           @transient private val conf: SparkConf,
                           endpointAddress: RpcEndpointAddress,
                           @transient @volatile private var nettyEnv: NettyRpcEnv)
  extends RpcEndpointRef(conf) with Serializable {

  @transient
  @volatile var client: TransportClient = _

  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  private val _name = endpointAddress.name

  override def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = _name

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${_address})"

  def toURI: URI = new URI(_address.toString)

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()
}

/**
  * The message that is sent from the sender to the receiver.
  */
case class RequestMessage(
                           senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any)

/**
  * A response that indicates some failure happens in the receiver side.
  */
case class RpcFailure(e: Throwable)

class NettyRpcEnv(val conf: SparkConf,
                  javaSerializerInstance: JavaSerializerInstance,
                  host: String,
                  securityManager: SecurityManager) extends RpcEnv(conf) {

  val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", 0))

  private val dispatcher: Dispatcher = new Dispatcher(this)

  // omit for signature
  private val streamManager = new StreamManager {
    override def getChunk(streamId: Long, chunkIndex: Int) = null
  }

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    //    if (securityManager.isAuthenticationEnabled()) {
    //      java.util.Arrays.asList(new SaslClientBootstrap(transportConf, "", securityManager,
    //        securityManager.isSaslEncryptionEnabled()))
    //    } else {
    java.util.Collections.emptyList[TransportClientBootstrap]
    //    }
  }

  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 64))

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
    * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
    * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
    */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
    * Remove the address's Outbox and stop it.
    */
  def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] = java.util.Collections.emptyList()
    server = transportContext.createServer(host, port, bootstraps)
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      dispatcher.postOneWayMessage(message)
    } else {
      // Message to a remote RPC endpoint.
      postToOutbox(message.receiver, OneWayOutboxMessage(serialize(message)))
    }
  }

  def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        println(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          println(s"Ignored message: $reply")
        }
    }

    if (remoteAddr == address) {
      val p = Promise[Any]()
      p.future.onComplete {
        case Success(response) => onSuccess(response)
        case Failure(e) => onFailure(e)
      }(ThreadUtils.sameThread)
      dispatcher.postLocalMessage(message, p)
    } else {
      val rpcMessage = RpcOutboxMessage(serialize(message),
        onFailure,
        (client, response) => onSuccess(deserialize[Any](client, response)))
      postToOutbox(message.receiver, rpcMessage)
      promise.future.onFailure {
        case _: TimeoutException => rpcMessage.onTimeout()
        case _ =>
      }(ThreadUtils.sameThread)
    }

    val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
      override def run(): Unit = {
        promise.tryFailure(
          new TimeoutException(s"Cannot receive any reply in ${timeout.duration}"))
      }
    }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
    promise.future.onComplete { v =>
      timeoutCancelable.cancel(true)
    }(ThreadUtils.sameThread)
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String =
    new RpcEndpointAddress(address, endpointName).toString

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
  }

  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

}

object NettyRpcEnv {

  /**
    * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
    * Use `currentEnv` to wrap the deserialization codes. E.g.,
    *
    * {{{
    *   NettyRpcEnv.currentEnv.withValue(this) {
    *     your deserialization codes
    *   }
    * }}}
    */
  val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
    * Similar to `currentEnv`, this variable references the client instance associated with an
    * RPC, in case it's needed to find out the remote address during deserialization.
    */
  val currentClient = new DynamicVariable[TransportClient](null)

}

case class RpcEndpointAddress(val rpcAddress: RpcAddress, val name: String) {

  require(name != null, "RpcEndpoint name must be provided.")

  def this(host: String, port: Int, name: String) = {
    this(RpcAddress(host, port), name)
  }

  override val toString = if (rpcAddress != null) {
    s"spark://$name@${rpcAddress.host}:${rpcAddress.port}"
  } else {
    s"spark-client://$name"
  }
}

object RpcEndpointAddress {

  def apply(sparkUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new SparkException("Invalid Spark URL: " + sparkUrl)
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid Spark URL: " + sparkUrl, e)
    }
  }
}

class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an [[RpcEndpoint]] exists. */
  case class CheckExistence(name: String)

}
