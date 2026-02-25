package com.pcpitman.mock.redis

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging

import io.lettuce.core.RedisFuture
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands

object MockRedisClient {
  def newProxy(): StatefulRedisConnection[String, String] = {
    val mock = new MockRedisClient()
    val commandsProxy = createAsyncCommandsProxy(mock)
    createConnectionProxy(commandsProxy)
  }

  private def createAsyncCommandsProxy(
    mock: MockRedisClient
  ): RedisAsyncCommands[String, String] = {
    val ctype = classOf[RedisAsyncCommands[String, String]]
    val handler = new InvocationHandler() {
      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        try {
          val m = mock.getClass().getMethod(method.getName(), method.getParameterTypes: _*)
          if (args == null) m.invoke(mock) else m.invoke(mock, args: _*)
        } catch {
          case e: NoSuchMethodException =>
            throw new UnsupportedOperationException(s"Mock ${method.getName()} not implemented")
        }
      }
    }
    Proxy
      .newProxyInstance(ctype.getClassLoader(), Array(ctype), handler)
      .asInstanceOf[RedisAsyncCommands[String, String]]
  }

  private def createConnectionProxy(
    commands: RedisAsyncCommands[String, String]
  ): StatefulRedisConnection[String, String] = {
    val ctype = classOf[StatefulRedisConnection[String, String]]
    val handler = new InvocationHandler() {
      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        method.getName match {
          case "async" => commands
          case "close" => () // no-op
          case _ =>
            throw new UnsupportedOperationException(
              s"Mock connection ${method.getName()} not implemented"
            )
        }
      }
    }
    Proxy
      .newProxyInstance(
        ctype.getClassLoader(),
        Array(ctype),
        handler
      )
      .asInstanceOf[StatefulRedisConnection[String, String]]
  }
}

class MockRedisClient extends LazyLogging {

  private val store = new ConcurrentHashMap[String, String]()

  logger.info("Created mock RedisClient")

  private def toRedisFuture[T](value: T): RedisFuture[T] = {
    val cf = CompletableFuture.completedFuture(value)
    val handler = new InvocationHandler() {
      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        method.getName match {
          case "getError"             => null
          case "await"                => java.lang.Boolean.TRUE
          case "toCompletableFuture"  => cf
          case _ =>
            val m = cf.getClass().getMethod(method.getName(), method.getParameterTypes: _*)
            if (args == null) m.invoke(cf) else m.invoke(cf, args: _*)
        }
      }
    }
    Proxy
      .newProxyInstance(classOf[RedisFuture[T]].getClassLoader(), Array(classOf[RedisFuture[T]]), handler)
      .asInstanceOf[RedisFuture[T]]
  }

  // Parameter types use Object due to generic type erasure on RedisAsyncCommands<K, V>

  def get(key: Object): RedisFuture[String] = {
    toRedisFuture(store.get(key.asInstanceOf[String]))
  }

  def set(key: Object, value: Object): RedisFuture[String] = {
    store.put(key.asInstanceOf[String], value.asInstanceOf[String])
    toRedisFuture("OK")
  }

  def setex(key: Object, seconds: Long, value: Object): RedisFuture[String] = {
    // TTL not enforced in mock — just store the value
    store.put(key.asInstanceOf[String], value.asInstanceOf[String])
    toRedisFuture("OK")
  }

  def del(keys: Array[Object]): RedisFuture[java.lang.Long] = {
    var count = 0L
    keys.foreach { k =>
      if (store.remove(k.asInstanceOf[String]) != null) count += 1
    }
    toRedisFuture(java.lang.Long.valueOf(count))
  }

  def close(): Unit = {}
}
