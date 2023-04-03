package com.msb.connector

import com.msb.utils.ConfigMap
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaStreamConnector {

  def createStream(streamingContext: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigMap.config(ConfigMap.kafkaServer),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ConfigMap.config(ConfigMap.kafkaGroupId),
      "auto.offset.reset" -> ConfigMap.config(ConfigMap.autoOffsetReset),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(ConfigMap.config(ConfigMap.kafkaTopicsIn))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
}
