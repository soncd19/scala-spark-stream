package com.msb.application
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConverters._
import java.sql.{DriverManager,SQLException}
import org.apache.spark.sql.streaming.Trigger
import java.sql.{CallableStatement}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.concurrent.{TimeUnit,TimeoutException}
import org.apache.spark.sql.DataFrame


object SparkReadDataFromOracleSendKafka {
  // KafkaInformation
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "10.1.66.33:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  ).asJava

  // OracleInformation
  val jdbcProps = Map[String, String](
    "user" -> "Landing",
    "password" -> "MSB@2021",
    "url" -> "jdbc:oracle:thin:@10.0.111.20:1521:BI")

  val spark = SparkSession.builder()
    .appName("SparkApplication")
    .master("local[*]")
    .getOrCreate()
  // Define the schema for the JSON data
  val schema = StructType(Seq(
    StructField("UserData", StringType),
    StructField("ActionArray", StringType),
    StructField("UserHeader", StringType)))
  //KafkaReader
  val kafkaServers = "10.1.66.33:9092"
  val kafkaTopicRead = "msb-kafka-service"
  val kafkaTopicWrite = "msb-kafka-service"
  //CreateKafkaConnection
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def KafkaMicroBatchUpsert(batchDf: DataFrame, batchId: Long): Unit = {
      if (batchDf.isEmpty) return
      batchDf.persist()
      val brConnect = spark.sparkContext.broadcast(jdbcProps)
      batchDf.foreachPartition((p: scala.collection.Iterator[org.apache.spark.sql.Row]) => {
          val connectionProps = brConnect.value
          val conn = DriverManager.getConnection(connectionProps("url"), connectionProps("user"), connectionProps("password"))
          try {
            val procedureName = "{ call PR_OUPUT_SERVICES(?, ?, ?) }"
            val stmt: CallableStatement = conn.prepareCall(procedureName)
            val producer = new KafkaProducer[String, String](kafkaParams)
            conn.setAutoCommit(false)
            p.foreach(r => {
                val para1 = r.getAs[String]("MyAction")
                val para2 = r.getAs[String]("UserData")
                val para3 = r.getAs[String]("ActionArray")
                val para4= """{"UserData":"""+para2+""","ActionArray":"""+para3
                stmt.setString(1, para1)//Parametter1
                stmt.setString(2,pretty(render(parse(para4))))
                stmt.registerOutParameter(3, java.sql.Types.CLOB)
                val _ = stmt.execute()
                val record: String = stmt.getString(3)
                //SenDataToKafkaHere
                producer.send(new ProducerRecord[String, String](kafkaTopicWrite, record))
                //FinshSendingdata
            })
            conn.commit()
            stmt.close()
            conn.close()
            producer.close()
          } catch {
              case a: SQLException => a.printStackTrace(); conn.rollback()
              case b: NullPointerException => b.printStackTrace(); conn.rollback()
          } finally {
            conn.close()
          }
      })
      batchDf.unpersist()
    }
  //////////////////////////////////////////////////////////////////////////////////////////////////////
  // Streaming
  val dfStream = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafkaServers)
    .option("subscribe", kafkaTopicRead)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp").select("key","value")
    .select(from_json(col("value").cast("string"), schema).alias("parsed_json"))
    .selectExpr("parsed_json.UserData as UserData",
      "parsed_json.ActionArray as ActionArray",
      "parsed_json.UserHeader as UserHeader")
    .withColumn("MyAction", regexp_extract(col("UserHeader"), "\"MyAction\":\"(.*?)\"", 1))
    .writeStream
    .foreachBatch(KafkaMicroBatchUpsert _)
    .queryName("SparkReadDataFromOracleSendKafka")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
    .option("checkpointLocation", "/yourpath/")
    .start()
    
  spark.streams.awaitAnyTermination()

}



