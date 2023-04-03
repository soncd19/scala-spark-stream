package com.msb

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[1]").setAppName("Spark Kafka Test")

    val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds.apply(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_stream_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val topics = Array("spark-kafka")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value()).print(10)

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
