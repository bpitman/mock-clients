package software.amazon.awssdk.services.ses

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Clock
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

import com.typesafe.scalalogging.LazyLogging

import software.amazon.awssdk.services.ses.model._

object MockSesAsyncClient {
  def newProxy(clock: Clock): SesAsyncClient = {
    val ctype = classOf[SesAsyncClient]
    val handler = new InvocationHandler() {
      val mock = new MockSesAsyncClient(clock)

      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        try {
          val m = mock.getClass().getDeclaredMethod(method.getName(), method.getParameterTypes()*)
          if (args == null) m.invoke(mock) else m.invoke(mock, args*)
        }
        catch {
          case e: NoSuchMethodException => {
            throw new UnsupportedOperationException(s"Mock ${method.getName()} not implemented")
          }
        }
      }
    }
    Proxy.newProxyInstance(ctype.getClassLoader(), Array(ctype), handler)
      .asInstanceOf[SesAsyncClient]
  }
}

class MockSesAsyncClient(clock: Clock) extends LazyLogging {

  val counter = new java.util.concurrent.atomic.AtomicInteger(0)

  def close(): Unit = {}

  def serviceName(): String = SesAsyncClient.SERVICE_NAME

  def sendEmail(request: SendEmailRequest): CompletableFuture[SendEmailResponse] = {
    CompletableFuture.supplyAsync(new Supplier[SendEmailResponse]() {
      override def get(): SendEmailResponse = {
        logger.info(s"sendEmail: ${request}")
        SendEmailResponse.builder().messageId(s"id-${counter.getAndIncrement}").build()
      }
    })
  }
}
