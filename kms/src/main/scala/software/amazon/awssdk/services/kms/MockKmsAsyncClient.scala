package software.amazon.awssdk.services.kms

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.ByteBuffer
import java.time.Clock
import java.util.Base64
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.spec.SecretKeySpec

import com.typesafe.scalalogging.LazyLogging

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kms.model._

object MockKmsAsyncClient {
  def newProxy(clock: Clock): KmsAsyncClient = {
    val ctype = classOf[KmsAsyncClient]
    val handler = new InvocationHandler() {
      val mock = new MockKmsAsyncClient(clock)

      override def invoke(proxy: Object, method: Method, args: Array[Any]): Any = {
        try {
          val m = mock.getClass().getMethod(method.getName(), method.getParameterTypes: _*)
          if (args == null) m.invoke(mock) else m.invoke(mock, args: _*)
        }
        catch {
          case e: NoSuchMethodException => {
            throw new java.lang.UnsupportedOperationException(s"Mock ${method.getName()} not implemented")
          }
        }
      }
    }
    Proxy.newProxyInstance(ctype.getClassLoader(), Array(ctype), handler)
      .asInstanceOf[KmsAsyncClient]
  }
}

case class MockKey(
  keyId: String,
  arn: String,
  description: String,
  creationDate: java.time.Instant,
  enabled: Boolean,
  keySpec: KeySpec,
  secretKey: javax.crypto.SecretKey
)

class MockKmsAsyncClient(clock: Clock) extends LazyLogging {

  val keys = new ConcurrentHashMap[String, MockKey]()
  val aliases = new ConcurrentHashMap[String, String]()

  logger.info("Created mock KmsAsyncClient")

  def close(): Unit = {}

  def serviceName(): String = KmsAsyncClient.SERVICE_NAME

  // Embed keyId in ciphertext so decrypt can find the key without an explicit keyId,
  // mimicking how real KMS embeds key metadata in the ciphertext blob.
  // Format: [36-byte UUID keyId][encrypted bytes]
  private val KeyIdLen = 36

  private def wrapCiphertext(keyId: String, encrypted: Array[Byte]): Array[Byte] = {
    keyId.getBytes("UTF-8") ++ encrypted
  }

  private def unwrapCiphertext(blob: Array[Byte]): (String, Array[Byte]) = {
    val keyId = new String(blob, 0, KeyIdLen, "UTF-8")
    val encrypted = blob.drop(KeyIdLen)
    (keyId, encrypted)
  }

  private def resolveKeyId(keyId: String): String = {
    if (keyId.startsWith("alias/")) aliases.get(keyId)
    else keyId
  }

  private def getKey(keyId: String): MockKey = {
    val resolved = resolveKeyId(keyId)
    val key = if (resolved != null) keys.get(resolved) else null
    if (key == null) {
      throw NotFoundException.builder()
        .message(s"Key $keyId does not exist")
        .build()
    }
    if (!key.enabled) {
      throw DisabledException.builder()
        .message(s"Key $keyId is disabled")
        .build()
    }
    key
  }

  private def keyMetadata(key: MockKey): KeyMetadata = {
    KeyMetadata.builder()
      .keyId(key.keyId)
      .arn(key.arn)
      .description(key.description)
      .creationDate(key.creationDate)
      .enabled(key.enabled)
      .keySpec(key.keySpec)
      .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
      .keyManager(KeyManagerType.CUSTOMER)
      .build()
  }

  def createKey(request: CreateKeyRequest): CompletableFuture[CreateKeyResponse] = {
    CompletableFuture.supplyAsync(new Supplier[CreateKeyResponse]() {
      override def get(): CreateKeyResponse = {
        val keyId = UUID.randomUUID().toString
        val arn = s"arn:aws:kms:us-east-1:000000000000:key/$keyId"
        val keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256)
        val secretKey = keyGen.generateKey()
        val description = Option(request.description).getOrElse("")
        val keySpec = Option(request.keySpec).getOrElse(KeySpec.SYMMETRIC_DEFAULT)
        val key = MockKey(keyId, arn, description, clock.instant(), true, keySpec, secretKey)
        keys.put(keyId, key)
        logger.info(s"Created key: $keyId")
        CreateKeyResponse.builder().keyMetadata(keyMetadata(key)).build()
      }
    })
  }

  def describeKey(request: DescribeKeyRequest): CompletableFuture[DescribeKeyResponse] = {
    CompletableFuture.supplyAsync(new Supplier[DescribeKeyResponse]() {
      override def get(): DescribeKeyResponse = {
        val key = getKey(request.keyId)
        DescribeKeyResponse.builder().keyMetadata(keyMetadata(key)).build()
      }
    })
  }

  def createAlias(request: CreateAliasRequest): CompletableFuture[CreateAliasResponse] = {
    CompletableFuture.supplyAsync(new Supplier[CreateAliasResponse]() {
      override def get(): CreateAliasResponse = {
        val key = getKey(request.targetKeyId)
        aliases.put(request.aliasName, key.keyId)
        logger.info(s"Created alias: ${request.aliasName} -> ${key.keyId}")
        CreateAliasResponse.builder().build()
      }
    })
  }

  def encrypt(request: EncryptRequest): CompletableFuture[EncryptResponse] = {
    CompletableFuture.supplyAsync(new Supplier[EncryptResponse]() {
      override def get(): EncryptResponse = {
        val key = getKey(request.keyId)
        val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
        cipher.init(Cipher.ENCRYPT_MODE, key.secretKey)
        val encrypted = cipher.doFinal(request.plaintext.asByteArray())
        EncryptResponse.builder()
          .keyId(key.keyId)
          .ciphertextBlob(SdkBytes.fromByteArray(wrapCiphertext(key.keyId, encrypted)))
          .build()
      }
    })
  }

  def decrypt(request: DecryptRequest): CompletableFuture[DecryptResponse] = {
    CompletableFuture.supplyAsync(new Supplier[DecryptResponse]() {
      override def get(): DecryptResponse = {
        val (embeddedKeyId, encrypted) = unwrapCiphertext(request.ciphertextBlob.asByteArray())
        val key = getKey(if (request.keyId != null) request.keyId else embeddedKeyId)
        val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
        cipher.init(Cipher.DECRYPT_MODE, key.secretKey)
        val plaintext = cipher.doFinal(encrypted)
        DecryptResponse.builder()
          .keyId(key.keyId)
          .plaintext(SdkBytes.fromByteArray(plaintext))
          .build()
      }
    })
  }

  def generateDataKey(
    request: GenerateDataKeyRequest
  ): CompletableFuture[GenerateDataKeyResponse] = {
    CompletableFuture.supplyAsync(new Supplier[GenerateDataKeyResponse]() {
      override def get(): GenerateDataKeyResponse = {
        val key = getKey(request.keyId)
        val keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256)
        val dataKey = keyGen.generateKey()
        val plaintext = dataKey.getEncoded
        val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
        cipher.init(Cipher.ENCRYPT_MODE, key.secretKey)
        val encrypted = cipher.doFinal(plaintext)
        GenerateDataKeyResponse.builder()
          .keyId(key.keyId)
          .plaintext(SdkBytes.fromByteArray(plaintext))
          .ciphertextBlob(SdkBytes.fromByteArray(wrapCiphertext(key.keyId, encrypted)))
          .build()
      }
    })
  }

  def scheduleKeyDeletion(
    request: ScheduleKeyDeletionRequest
  ): CompletableFuture[ScheduleKeyDeletionResponse] = {
    CompletableFuture.supplyAsync(new Supplier[ScheduleKeyDeletionResponse]() {
      override def get(): ScheduleKeyDeletionResponse = {
        val resolved = resolveKeyId(request.keyId)
        val key = keys.get(resolved)
        if (key == null) {
          throw NotFoundException.builder()
            .message(s"Key ${request.keyId} does not exist")
            .build()
        }
        keys.put(resolved, key.copy(enabled = false))
        val days = Option(request.pendingWindowInDays: Integer).map(_.intValue).getOrElse(30)
        val deletionDate = clock.instant().plusSeconds(days.toLong * 86400)
        logger.info(s"Scheduled key deletion: ${key.keyId}, date=$deletionDate")
        ScheduleKeyDeletionResponse.builder()
          .keyId(key.keyId)
          .deletionDate(deletionDate)
          .build()
      }
    })
  }
}
