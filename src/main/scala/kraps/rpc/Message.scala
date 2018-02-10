package kraps.rpc

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}

import scala.util.control.NonFatal


sealed trait InboxMessage

case class OneWayMessage(
                          senderAddress: RpcAddress,
                          content: Any) extends InboxMessage

case class RpcMessage(
                       senderAddress: RpcAddress,
                       content: Any,
                       context: NettyRpcCallContext) extends InboxMessage

case object OnStart extends InboxMessage

case object OnStop extends InboxMessage

case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
  * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
  */
class Inbox(
             val endpointRef: NettyRpcEndpointRef,
             val endpoint: RpcEndpoint) {

  inbox =>
  // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
    * Process stored messages.
    */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new RuntimeException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new RuntimeException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized {
              inbox.numActiveThreads
            }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized {
    messages.isEmpty
  }

  /**
    * Called when we are dropping a message. Test cases override this to test message dropping.
    * Exposed for testing.
    */
  protected def onDrop(message: InboxMessage): Unit = {
    println(s"Drop $message because $endpointRef is stopped")
  }

  /**
    * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
    */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => println(s"Ignoring error", ee)
        }
    }
  }

}

sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    println(s"Failed to send one-way RPC.", e)
  }

}

case class RpcOutboxMessage(
                                            content: ByteBuffer,
                                            _onFailure: (Throwable) => Unit,
                                            _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    require(client != null, "TransportClient has not yet been set.")
    client.removeRpcRequest(requestId)
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox =>
  // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  private var client: TransportClient = null

  /**
    * connectFuture points to the connect task. If there is no connect task, connectFuture will be
    * null.
    */
  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  private var stopped = false

  /**
    * If there is any thread draining the message queue
    */
  @GuardedBy("this")
  private var draining = false

  /**
    * Send a message. If there is no active connection, cache it and launch a new connection. If
    * [[Outbox]] is stopped, the sender will be notified with a [[RuntimeException]].
    */
  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new RuntimeException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
    * Drain the message queue. If there is other draining thread, just exit. If the connection has
    * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
    * connection.
    */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized {
          client
        }
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized {
              connectFuture = null
            }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized {
          connectFuture = null
        }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      }
    })
  }

  /**
    * Stop [[Inbox]] and notify the waiting messages with the cause.
    */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    // Not sure if `client.close` is idempotent. Just for safety.
    if (client != null) {
      client.close()
    }
    client = null
  }

  /**
    * Stop [[Outbox]]. The remaining messages in the [[Outbox]] will be notified with a
    * [[RuntimeException]].
    */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new RuntimeException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
