package com.msb.pub

import com.msb.utils.Logging
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.Serializable
import java.util

case class RedisExecute(redis_topic: String, redis_config_path: String) extends Serializable with Logging {

  private lazy val connection: RedisConnection = connect()

  def connect(): RedisConnection = {
    logInfo("created redis connection")
    new RedisConnection(redis_config_path)
  }

  def close() = {
    connection.close()
  }

  def process(record: ConsumerRecord[String, String]): Unit = {

    logInfo("record.value():" + record.value())
    val messageBody: util.Map[String, String] = new util.HashMap[String, String]
    messageBody.put("json_in", record.value())
    messageBody.put("value_ts", String.valueOf(System.currentTimeMillis))

    connection.xAdd(record, redis_topic)

    logInfo("xadd data to redis")
  }
}
