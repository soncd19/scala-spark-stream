package com.msb.connector

import com.msb.call.CallProducerV2
import com.msb.utils.{Logging, PropertiesFileReader, StringToJson}
import com.msb.redis.provider.redis.streaming.StreamItem
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.io.Serializable
import java.sql.Connection

case class DatabaseConnector(config_path: String) extends Serializable with Logging {

  private lazy val callProducerV2: CallProducerV2 = init()

  def init(): CallProducerV2 = {
    val config = new HikariConfig()
    val properties = PropertiesFileReader.readConfig(config_path)
    config.setJdbcUrl(properties.getProperty("dataSource.uri"))
    config.setUsername(properties.getProperty("dataSource.user"))
    config.setPassword(properties.getProperty("dataSource.password"))
    config.setDriverClassName("oracle.jdbc.driver.OracleDriver")
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    new CallProducerV2(new HikariDataSource(config).getConnection())
  }

  def process(item: StreamItem): String = {

    val json = StringToJson.toJson(item.fields("json_in"))
    val action = json.getAsJsonObject("UserData").get("MyAction").getAsString
    val response = callProducerV2.callProduce(action, json.toString)
    logDebug(response)
    response
  }

  def close() = {
    callProducerV2.close()
  }

}
