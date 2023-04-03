package com.msb.utils

object ConfigMap {

  var jobName ="jobName"
  var kafkaServer = "kafkaServer"
  var kafkaGroupId = "kafkaGroupId"
  var autoOffsetReset = "autoOffsetReset"
  var redisServer = "redisServer"
  var kafkaTopicsIn = "kafkaTopicsIn"
  var kafkaTopicsOut = "kafkaTopicsOut"
  var redisTopics = "redisTopics"
  var redisConfigPath = "redisConfigPath"
  var redisConsumerGroup = "redisConsumerGroup"
  var redisConsumerName = "redisConsumerName"

  var config: Map[String, String] = {
    Map()
  }
}
