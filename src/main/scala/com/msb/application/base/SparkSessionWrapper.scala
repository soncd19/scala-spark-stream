package com.msb.application.base

import com.msb.utils.ConfigMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName(ConfigMap.config(ConfigMap.jobName))

    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
  }

}
