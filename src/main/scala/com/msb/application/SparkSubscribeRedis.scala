package com.msb.application

import com.msb.application.base.BaseApplication
import com.msb.connector.DatabaseConnector
import com.msb.pub.SparkKafkaProducer
import com.msb.utils.{ConfigMap, StringToJson}
import com.msb.redis.provider.redis.streaming.{ConsumerConfig, StreamItem, toRedisStreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class SparkSubscribeRedis(configPath: String) extends BaseApplication("SparkSubscribeRedis") {

  loadConfig(configPath)

  override def run(): Unit = {

    val spark = SparkSession.builder.appName("Redis Stream Example")
      .master("local[*]")
      .config("spark.redis.mode", "sentinel")
      .config("spark.redis.sentinel.master", "transfer247")
      .config("spark.redis.auth", "msb@2022")
      .config("spark.redis.host", "10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("INFO")

    val sparkProducer = sparkContext.broadcast(SparkKafkaProducer(ConfigMap.config(ConfigMap.kafkaServer),
      ConfigMap.config(ConfigMap.kafkaTopicsOut)))

    val databaseConnector = sparkContext.broadcast(DatabaseConnector(configPath))

    logInfo("creating SparkSubscribeRedis")
    try {

      val ssc = new StreamingContext(sparkContext, Milliseconds(100))
      logInfo("created SparkSubscribeRedis")

      val stream = ssc.createRedisXStream(Seq(ConsumerConfig(ConfigMap.config(ConfigMap.redisTopics),
        ConfigMap.config(ConfigMap.redisConsumerGroup), ConfigMap.config(ConfigMap.redisConsumerName)))
        , storageLevel = StorageLevel.MEMORY_AND_DISK_2)

      stream.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          logInfo("receiver data redis")
          partitionOfRecords.foreach(record => {
            val response = databaseConnector.value.process(record)
            sparkProducer.value.send(response)
          })
        }
      }

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case ex: Exception => logError(ex.getMessage)
      case _: Throwable => println("Got some other kind of Throwable exception")
    } finally {
      databaseConnector.value.close()
      sparkProducer.value.close()
      shutDown()
    }

  }

  def process(item: StreamItem): Unit = {
    val json = StringToJson.toJson(item.fields("json_in"))
    logInfo(json.toString)
  }


}
