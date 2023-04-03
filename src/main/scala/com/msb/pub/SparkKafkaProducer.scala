package com.msb.pub

import com.msb.utils.Logging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import java.io.Serializable
import java.util.Properties

case class SparkKafkaProducer(kafka_broker: String, out_topic: String) extends Serializable with Logging {

  private lazy val producer: KafkaProducer[Array[Byte], Array[Byte]] = init()

  private def init(): KafkaProducer[Array[Byte], Array[Byte]] = {
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", kafka_broker)
    properties.put("client.id", "spark-producer")
    properties.put("acks", "all")
    properties.put("retries", "3")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    sys.addShutdownHook {
      producer.close()
    }
    new KafkaProducer[Array[Byte], Array[Byte]](properties)
  }

  def close(): Unit = producer.close()

  def send(message: String): Unit = {

    try {
      val record = new ProducerRecord(out_topic, getKey(System.currentTimeMillis()), getMessage(message))
      val future = producer.send(record, new DummyCallback())
    }
    catch {
      case ex: Exception => logError(ex.getMessage)
      case _: Throwable => println("Sending message to kafka error")
    }
  }


  private def getKey(i: Long): Array[Byte] = {
    serialize(String.valueOf(i))
  }

  private def getMessage(message: String) = {
    serialize(message)
  }

  private def serialize(obj: AnyRef) = {
    org.apache.commons.lang3.SerializationUtils.serialize(obj.asInstanceOf[Serializable])
  }

  private class DummyCallback extends Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        logError("Error while producing message to topic : " + recordMetadata.topic)
      }
      else {
        logDebug("sent message to topic, partition, offset: " + recordMetadata.topic + ", "
          + recordMetadata.partition + ", " + recordMetadata.offset)
      }
    }
  }


}
