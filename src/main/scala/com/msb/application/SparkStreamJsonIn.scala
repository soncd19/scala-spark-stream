package com.msb.application

import com.msb.application.base.BaseApplication
import com.msb.connector.KafkaStreamConnector
import com.msb.pub.RedisExecute
import com.msb.utils.ConfigMap
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class SparkStreamJsonIn(configPath: String, redisConfigPath: String)
  extends BaseApplication("SparkKafkaStreamProcess") {

  loadConfig(configPath)

  override def run(): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    logInfo("starting create redis pool")

    val redisExecute = spark.sparkContext.broadcast(RedisExecute(ConfigMap.config(ConfigMap.redisTopics), redisConfigPath))

    try {

      val streamingContext = new StreamingContext(spark.sparkContext, Milliseconds.apply(100))

      val stream = KafkaStreamConnector.createStream(streamingContext)

      logInfo("created stream kafka")

      stream.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          partitionOfRecords.foreach(record => redisExecute.value.process(record))
        }

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
      logInfo("streamingContext started")
      streamingContext.start()
      streamingContext.awaitTermination()

    }
    catch {
      case ex: Exception => logError(ex.getMessage)
      case _: Throwable => println("Got some other kind of Throwable exception")
    } finally {
      redisExecute.value.close()
      shutDown()
    }

  }
}
