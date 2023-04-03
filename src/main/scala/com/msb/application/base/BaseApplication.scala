package com.msb.application.base

import com.msb.utils.{ConfigMap, Logging}
import com.msb.utils.PropertiesFileReader.readConfig
import org.apache.spark.sql.{Dataset, Row}

abstract class BaseApplication(jobName: String) extends SparkSessionWrapper with Logging {

  protected def loadConfig(configPath: String): Unit = {
    val properties = readConfig(configPath)

    val kafkaServer = properties.getProperty("kafka.server")
    val redisServer = properties.getProperty("redis.server")
    val kafkaTopicsIn = properties.getProperty("kafka.topics.in")
    val kafkaTopicsOut = properties.getProperty("kafka.topics.out")
    val redisTopics = properties.getProperty("redis.topics")
    val kafkaGroupId = properties.getProperty("kafka.groupId")
    val autoOffsetReset = properties.getProperty("auto.offset.reset")
    val redisConfigPath = properties.getProperty("redis.config.path")
    val redisConsumerGroup = properties.getProperty("redis.consumer.group")
    val redisConsumerName = properties.getProperty("redis.consumer.name")
    ConfigMap.config += (ConfigMap.jobName -> jobName)
    ConfigMap.config += (ConfigMap.kafkaServer -> kafkaServer)
    ConfigMap.config += (ConfigMap.redisServer -> redisServer)
    ConfigMap.config += (ConfigMap.kafkaTopicsIn -> kafkaTopicsIn)
    ConfigMap.config += (ConfigMap.kafkaTopicsOut -> kafkaTopicsOut)
    ConfigMap.config += (ConfigMap.redisTopics -> redisTopics)
    ConfigMap.config += (ConfigMap.kafkaGroupId -> kafkaGroupId)
    ConfigMap.config += (ConfigMap.autoOffsetReset -> autoOffsetReset)
    ConfigMap.config += (ConfigMap.redisConfigPath -> redisConfigPath)
    ConfigMap.config += (ConfigMap.redisConsumerGroup -> redisConsumerGroup)
    ConfigMap.config += (ConfigMap.redisConsumerName -> redisConsumerName)
  }


    protected def readJsonData(folderPath: String): Dataset[Row] = {
      spark
        .read
        .json(folderPath)
    }

    protected def readParquetData(folderPath: String): Dataset[Row] = {
      spark
        .read
        .parquet(folderPath)
    }

  protected def shutDown(): Unit = {
    spark.stop()
  }

  def run(): Unit

}
