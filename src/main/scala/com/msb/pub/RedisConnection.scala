package com.msb.pub

import com.msb.redis.api.{DistributedMapCacheClient, Serializer}
import com.msb.redis.serializer.StringSerializer
import com.msb.redis.service.RedisDistributedMapCacheClientService
import com.msb.utils.Logging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

import java.io.{IOException, Serializable}
import java.nio.charset.StandardCharsets
import java.util

class RedisConnection(redisConfigPath: String) extends Serializable with Logging {

  private val redisMapCacheClient: DistributedMapCacheClient = init()

  private def init(): RedisDistributedMapCacheClientService = {
    logInfo("created redis connection")
    new RedisDistributedMapCacheClientService(redisConfigPath)
  }

  @throws[IOException]
  def xAdd(record: ConsumerRecord[String, String], redisTopic: String): Unit = {
    val value: String = record.value
    val messageBody: util.Map[Array[Byte], Array[Byte]] = new util.HashMap[Array[Byte], Array[Byte]]
    messageBody.put("json_in".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))
    messageBody.put("value_ts".getBytes(StandardCharsets.UTF_8), String.valueOf(System.currentTimeMillis).getBytes(StandardCharsets.UTF_8))
    redisMapCacheClient.xAdd(redisTopic.getBytes(StandardCharsets.UTF_8), messageBody)
  }


  @throws[IOException]
  def publish(record: ConsumerRecord[String, String], channel: String): Unit = {
    val value: String = record.value
    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer
    redisMapCacheClient.publish(channel, value, keySerializer, valueSerializer)

  }

  def getRedisConnection: DistributedMapCacheClient = redisMapCacheClient

  def close(): Unit = {
    logInfo("close redis connection")
    redisMapCacheClient.disable()
  }

}
