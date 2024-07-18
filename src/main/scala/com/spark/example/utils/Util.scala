package com.spark.example.utils

import org.apache.spark.sql.SparkSession

object Util {

  def GetSparkSession(appName: String, conf: Map[String, Any]): SparkSession = {
    var session: SparkSession = null
    if (conf.isEmpty) {
      session = SparkSession
        .builder()
        .master("local[*]")
        .appName(appName)
        .getOrCreate()
    } else {
      session = SparkSession
        .builder()
        .appName(appName)
        .master("local[*]")
        .config(map = conf)
        .getOrCreate()
    }
    session
  }

}
