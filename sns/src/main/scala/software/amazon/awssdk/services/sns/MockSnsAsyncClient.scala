package software.amazon.awssdk.services.sns

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Clock
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier


import com.typesafe.scalalogging.LazyLogging

import software.amazon.awssdk.services.sns.model._

object MockSnsAsyncClient {
  def newProxy(clock: Clock): SnsAsyncClient = {
    val ctype = classOf[SnsAsyncClient]
    val handler = new InvocationHandler() {
      val mock = new MockSnsAsyncClient(clock)

      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        try {
          val m = mock.getClass().getMethod(method.getName(), method.getParameterTypes: _*)
          if (args == null) m.invoke(mock) else m.invoke(mock, args: _*)
        }
        catch {
          case e: NoSuchMethodException => {
            throw new UnsupportedOperationException(s"Mock ${method.getName()} not implemented")
          }
        }
      }
    }
    Proxy.newProxyInstance(ctype.getClassLoader(), Array(ctype), handler)
      .asInstanceOf[SnsAsyncClient]
  }
}

class MockSnsAsyncClient(clock: Clock) extends LazyLogging {

  val counter = new java.util.concurrent.atomic.AtomicInteger(0)

  @volatile var lastPublishedMessage: String = null
  @volatile var lastPublishedPhoneNumber: String = null

  def close(): Unit = {}

  def serviceName(): String = SnsAsyncClient.SERVICE_NAME

  def publish(request: PublishRequest): CompletableFuture[PublishResponse] = {
    CompletableFuture.supplyAsync(new Supplier[PublishResponse]() {
      override def get(): PublishResponse = {
        lastPublishedMessage = request.message()
        lastPublishedPhoneNumber = request.phoneNumber()
        logger.info(s"publish: phoneNumber=${request.phoneNumber()}, message=${request.message()}")
        PublishResponse.builder().messageId(s"id-${counter.getAndIncrement}").build()
      }
    })
  }
}
