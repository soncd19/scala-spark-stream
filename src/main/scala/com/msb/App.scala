package com.msb

import com.msb.application.{SparkStreamJsonIn, SparkSubscribeRedis}
import org.apache.log4j.{Level, Logger}

/**
 * @author soncd2
 */
object App {

  def main(args: Array[String]): Unit = {
    showCommand()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val command = if (args.length == 0) "process_json_in" else args(0)
    val configPath = if (args.length == 0) "D:\\Code\\spark\\msb-kafka-spark-stream\\config\\application.properties" else args(1)
    val redisConfigPath = if (args.length == 0) "D:\\Code\\spark\\msb-kafka-spark-stream\\config\\redis-config.xml" else args(2)
    command match {
      case "process_json_in" => new SparkStreamJsonIn(configPath, redisConfigPath).run()
      case "process_spark_sub_redis" => new SparkSubscribeRedis(configPath).run()
    }
  }

  private def showCommand(): Unit = {
    val introduction =
      """
        |WELCOME TO MSB KAFKA SERVICE SPARK STREAM
        |- 1 argument: Spark job command
        |- 2 argument: Application configuration path
        |Available spark job commands:
        |- process_json_in => SparkStreamJsonIn
        | - process_spark_sub_redis => SparkSubscribeRedis
        |""".stripMargin

    println(introduction)
  }

}
