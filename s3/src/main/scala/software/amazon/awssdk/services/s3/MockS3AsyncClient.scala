package software.amazon.awssdk.services.s3

import scala.jdk.CollectionConverters._

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.ByteBuffer
import java.time.Clock
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger

import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.model._

object MockS3AsyncClient extends LazyLogging {
  def newProxy(clock: Clock): S3AsyncClient = {
    val ctype = classOf[S3AsyncClient]
    val handler = new InvocationHandler() {
      val mock = new MockS3AsyncClient(logger, clock)

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
      .asInstanceOf[S3AsyncClient]
  }
}

case class BucketItem(
  contentEncoding: String,
  contentLength: Long,
  contentType: String,
  metadata: Map[String,String],
  bytes: Array[Byte]
)

class MockS3AsyncClient(logger: Logger, clock: Clock) {

  val buckets = new ConcurrentHashMap[String,ConcurrentHashMap[String,BucketItem]]

  def close(): Unit = {}

  def serviceName(): String = S3AsyncClient.SERVICE_NAME

  def createBucket(request: CreateBucketRequest): CompletableFuture[CreateBucketResponse] = {
    CompletableFuture.supplyAsync(new Supplier[CreateBucketResponse]() {
      override def get(): CreateBucketResponse = {
        if (buckets.putIfAbsent(request.bucket, new ConcurrentHashMap[String,BucketItem]) != null) {
          throw BucketAlreadyExistsException.builder()
            .message(s"Bucket ${request.bucket} already exists")
            .build()
        }
        logger.info(s"Bucket created: s3://${request.bucket}")
        CreateBucketResponse.builder().location("us-west-2").build()
      }
    })
  }

  def getObject[T](
    request: GetObjectRequest,
    transformer: AsyncResponseTransformer[GetObjectResponse,T]
  ): CompletableFuture[T] = {
    val future = transformer.prepare()
    CompletableFuture.supplyAsync(new Supplier[Boolean]() {
      override def get(): Boolean = {
        Option(buckets.get(request.bucket)) match {
          case Some(bucket) => {
            Option(bucket.get(request.key)) match {
              case Some(item) => {
                transformer.onResponse(
                  GetObjectResponse.builder()
                    .contentEncoding(item.contentEncoding)
                    .contentLength(item.contentLength)
                    .contentType(item.contentType)
                    .metadata(item.metadata.asJava)
                    .build()
                )
                transformer.onStream(AsyncRequestBody.fromBytes(item.bytes))
                true
              }
              case None => {
                transformer.exceptionOccurred(NoSuchKeyException.builder()
                  .message(s"Key ${request.key} does not exist in bucket ${request.bucket}")
                  .build()
                )
                false
              }
            }
          }
          case None => {
            transformer.exceptionOccurred(NoSuchBucketException.builder()
              .message(s"Bucket ${request.bucket} does not exist")
              .build()
            )
            false
          }
        }
      }
    }).thenCompose(response => future)
  }

  def putObject(
    request: PutObjectRequest,
    requestBody: AsyncRequestBody
  ): CompletableFuture[PutObjectResponse] = {
    CompletableFuture.supplyAsync(new Supplier[PutObjectResponse]() {
      override def get(): PutObjectResponse = {
        Option(buckets.get(request.bucket)) match {
          case Some(bucket) => {
            val body = new Array[Byte](request.contentLength().toInt)
            val latch = new CountDownLatch(1)
            var pos = 0
            requestBody.subscribe(new org.reactivestreams.Subscriber[ByteBuffer] {
              var subscription: org.reactivestreams.Subscription = scala.compiletime.uninitialized
              override def onSubscribe(s: org.reactivestreams.Subscription): Unit = {
                subscription = s
                s.request(Long.MaxValue)
              }
              override def onNext(byteBuffer: ByteBuffer): Unit = {
                val remaining = byteBuffer.remaining()
                byteBuffer.get(body, pos, remaining)
                pos += remaining
              }
              override def onError(t: Throwable): Unit = latch.countDown()
              override def onComplete(): Unit = latch.countDown()
            })
            latch.await(5, TimeUnit.SECONDS)
            val item = BucketItem(
              request.contentEncoding,
              request.contentLength,
              request.contentType,
              request.metadata.asScala.toMap,
              body.toArray
            )
            bucket.put(request.key, item)
            logger.info(s"Put object: ${request.bucket} / ${request.key}")
            PutObjectResponse.builder().build()
          }
          case None => {
            throw NoSuchBucketException.builder()
              .message(s"Bucket ${request.bucket} does not exist")
              .build()
          }
        }
      }
    })
  }
}
