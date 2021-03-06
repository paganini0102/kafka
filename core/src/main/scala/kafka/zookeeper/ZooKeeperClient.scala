/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.zookeeper

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, Semaphore, TimeUnit}

import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConverters._

/**
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking.
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int,
                      time: Time) extends Logging {
  this.logIdent = "[ZooKeeperClient] "
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala
  private val inFlightRequests = new Semaphore(maxInFlightRequests)
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala

  info(s"Initializing a new session to $connectString.")
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)
  waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)

  /**
   * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single request to send and wait on.
   * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  /**
   * Send a pipelined sequence of requests and wait for all of their responses.
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the request.
   *
   * @param requests a sequence of requests to send and wait on.
   * @return the responses for the requests. If all requests have the same type, the responses will have the respective
   * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = inReadLock(initializationLock) {
    if (requests.isEmpty)
      Seq.empty
    else {
      val countDownLatch = new CountDownLatch(requests.size)
      val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)

      requests.foreach { request =>
        inFlightRequests.acquire()
        try {
          send(request) { response =>
            responseQueue.add(response)
            inFlightRequests.release()
            countDownLatch.countDown()
          }
        } catch {
          case e: Throwable =>
            inFlightRequests.release()
            throw e
        }
      }
      countDownLatch.await()
      responseQueue.asScala.toBuffer
    }
  }

  private def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    // Safe to cast as we always create a response of the right type
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    def responseMetadata(sendTimeMs: Long) = new ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    val sendTimeMs = time.hiResClockMs()
    request match {
      case ExistsRequest(path, ctx) =>
        zooKeeper.exists(path, shouldWatch(request), new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(ExistsResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetDataRequest(path, ctx) =>
        zooKeeper.getData(path, shouldWatch(request), new DataCallback {
          override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
            callback(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetChildrenRequest(path, ctx) =>
        zooKeeper.getChildren(path, shouldWatch(request), new Children2Callback {
          override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat): Unit =
            callback(GetChildrenResponse(Code.get(rc), path, Option(ctx),
              Option(children).map(_.asScala).getOrElse(Seq.empty), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case CreateRequest(path, data, acl, createMode, ctx) =>
        zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
          override def processResult(rc: Int, path: String, ctx: Any, name: String): Unit =
            callback(CreateResponse(Code.get(rc), path, Option(ctx), name, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case SetDataRequest(path, data, version, ctx) =>
        zooKeeper.setData(path, data, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetDataResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case DeleteRequest(path, version, ctx) =>
        zooKeeper.delete(path, version, new VoidCallback {
          override def processResult(rc: Int, path: String, ctx: Any): Unit =
            callback(DeleteResponse(Code.get(rc), path, Option(ctx), responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetAclRequest(path, ctx) =>
        zooKeeper.getACL(path, null, new ACLCallback {
          override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
            callback(GetAclResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs)))
        }}, ctx.orNull)
      case SetAclRequest(path, acl, version, ctx) =>
        zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetAclResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
    }
  }

  /**
   * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
   * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
   * @throws ZooKeeperClientExpiredException if the session expired either before or while waiting for connection.
   */
  def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    waitUntilConnected(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  private def waitUntilConnected(timeout: Long, timeUnit: TimeUnit): Unit = {
    info("Waiting until connected.")
    var nanos = timeUnit.toNanos(timeout)
    inLock(isConnectedOrExpiredLock) {
      var state = zooKeeper.getState
      while (!state.isConnected && state.isAlive) {
        if (nanos <= 0) {
          throw new ZooKeeperClientTimeoutException(s"Timed out waiting for connection while in state: $state")
        }
        nanos = isConnectedOrExpiredCondition.awaitNanos(nanos)
        state = zooKeeper.getState
      }
      if (state == States.AUTH_FAILED) {
        throw new ZooKeeperClientAuthFailedException("Auth failed either before or while waiting for connection")
      } else if (state == States.CLOSED) {
        throw new ZooKeeperClientExpiredException("Session expired either before or while waiting for connection")
      }
    }
    info("Connected.")
  }

  // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
  // may need to be updated.
  private def shouldWatch(request: AsyncRequest): Boolean = request match {
    case _: GetChildrenRequest => zNodeChildChangeHandlers.contains(request.path)
    case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(request.path)
    case _ => throw new IllegalArgumentException(s"Request $request is not watchable")
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path, zNodeChildChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  /**
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = inReadLock(initializationLock) {
    if (stateChangeHandler != null)
      stateChangeHandlers.put(stateChangeHandler.name, stateChangeHandler)
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = inReadLock(initializationLock) {
    stateChangeHandlers.remove(name)
  }

  def close(): Unit = inWriteLock(initializationLock) {
    info("Closing.")
    zNodeChangeHandlers.clear()
    zNodeChildChangeHandlers.clear()
    stateChangeHandlers.clear()
    zooKeeper.close()
    info("Closed.")
  }

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
  }

  private def initialize(): Unit = {
    if (!zooKeeper.getState.isAlive) {
      info(s"Initializing a new session to $connectString.")
      var now = System.currentTimeMillis()
      val threshold = now + connectionTimeoutMs
      while (now < threshold) {
        try {
          zooKeeper.close()
          zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)
          waitUntilConnected(threshold - now, TimeUnit.MILLISECONDS)
          return
        } catch {
          case _: Exception =>
            now = System.currentTimeMillis()
            if (now < threshold) {
              Thread.sleep(1000)
              now = System.currentTimeMillis()
            }
        }
      }
      info(s"Timed out waiting for connection during session initialization while in state: ${zooKeeper.getState}")
      stateChangeHandlers.foreach {case (name, handler) => handler.onReconnectionTimeout()}
    }
  }

  /**
   * reinitialize method to use in unit tests
   */
  private[zookeeper] def reinitialize(): Unit = {
    zooKeeper.close()
    initialize()
  }

  private object ZooKeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug("Received event: " + event)
      Option(event.getPath) match {
        case None =>
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (event.getState == KeeperState.AuthFailed) {
            error("Auth failed.")
            stateChangeHandlers.foreach {case (name, handler) => handler.onAuthFailure()}
          } else if (event.getState == KeeperState.Expired) {
            inWriteLock(initializationLock) {
              info("Session expired.")
              stateChangeHandlers.foreach {case (name, handler) => handler.beforeInitializingSession()}
              initialize()
              stateChangeHandlers.foreach {case (name, handler) => handler.afterInitializingSession()}
            }
          }
        case Some(path) =>
          (event.getType: @unchecked) match {
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }
}

trait StateChangeHandler {
  val name: String
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
  def onReconnectionTimeout(): Unit = {}
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}

sealed trait AsyncRequest {
  /**
   * This type member allows us to define methods that take requests and return responses with the correct types.
   * See ``ZooKeeperClient.handleRequests`` for example.
   */
  type Response <: AsyncResponse
  def path: String
  def ctx: Option[Any]
}

case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode,
                         ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse
}

case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse
}

case class ExistsRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = ExistsResponse
}

case class GetDataRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetDataResponse
}

case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse
}

case class GetAclRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetAclResponse
}

case class SetAclRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetAclResponse
}

case class GetChildrenRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
}

sealed abstract class AsyncResponse {
  def resultCode: Code
  def path: String
  def ctx: Option[Any]

  /** Return None if the result code is OK and KeeperException otherwise. */
  def resultException: Option[KeeperException] =
    if (resultCode == Code.OK) None else Some(KeeperException.create(resultCode, path))

  /**
   * Throw KeeperException if the result code is not OK.
   */
  def maybeThrow(): Unit = {
    if (resultCode != Code.OK)
      throw KeeperException.create(resultCode, path)
  }

  def metadata: ResponseMetadata
}

case class ResponseMetadata(sendTimeMs: Long, receivedTimeMs: Long) {
  def responseTimeMs: Long = sendTimeMs - receivedTimeMs
}

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String, metadata: ResponseMetadata) extends AsyncResponse
case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any], metadata: ResponseMetadata) extends AsyncResponse
case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse

class ZooKeeperClientException(message: String) extends RuntimeException(message)
class ZooKeeperClientExpiredException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientAuthFailedException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientTimeoutException(message: String) extends ZooKeeperClientException(message)
