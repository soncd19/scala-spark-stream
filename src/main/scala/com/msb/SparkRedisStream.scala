package com.msb


import com.msb.utils.{StringToJson}
import com.msb.redis.provider.redis.streaming.{ConsumerConfig, StreamItem, toRedisStreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object SparkRedisStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Redis Stream Example")
      .master("local[*]")
      .config("spark.redis.mode", "sentinel")
      .config("spark.redis.sentinel.master", "transfer247")
      .config("spark.redis.auth", "msb@2022")
      .config("spark.redis.host", "10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(100))

    val stream = ssc.createRedisXStream(Seq(ConsumerConfig("redis-pub", "my-consumer-group", "my-consumer-1")))

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach(record => process(record))
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }


  def process(item: StreamItem): Unit = {
    val json = StringToJson.toJson(item.fields("json_in"))
    println(json.toString)
  }

}
