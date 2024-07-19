package com.spark.example.kafka

import com.spark.example.entities.{Customer, Order, Stock}
import com.spark.example.utils.Util
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}

object WriteToKafka {

  def main(args: Array[String]): Unit = {
    val spark = Util.GetSparkSession("KafkaWriter", Map())
    spark.sparkContext.setLogLevel(logLevel = "ERROR")

    val customerDs = processCustomerData(spark)
    writeToKafka(customerDs.writeStream, "customers","data/kafka/checkpoint/customers").start()

    val stockDs = processStocksData(spark)
    writeToKafka(stockDs.writeStream, "stocks","data/kafka/checkpoint/stocks").start()

    val orderDs = processOrdersData(spark)
    writeToKafka(orderDs.writeStream, "orders","data/kafka/checkpoint/orders").start()

    spark.streams.awaitAnyTermination()
  }

  private def processCustomerData(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val custerSchema = Encoders.product[Customer].schema
    val dataStream = spark
      .readStream
      .option("header", true)
      .schema(custerSchema)
      .csv("data/kafka/data/customers")

    dataStream
      .withColumn("value", functions.to_json(
        functions.struct($"custid", $"name", $"age")
      ))
      .withColumn("key", $"custid")
  }

  private def processStocksData(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val stockSchema = Encoders.product[Stock].schema
    val dataStream = spark
      .readStream
      .option("header", true)
      .schema(stockSchema)
      .csv("data/kafka/data/stocks")
    dataStream
      .withColumn("key", $"stocksymbol")
      .withColumn("value", functions.to_json(
        functions.struct($"stocksymbol", $"stockname")
      ))

  }

  private def processOrdersData(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val orderSchema = Encoders.product[Order].schema
    val dataStream = spark
      .readStream
      .option("header", true)
      .schema(orderSchema)
      .csv("data/kafka/data/orders")
    dataStream
      .withColumn("key", $"orderid".cast("string"))
      .withColumn("value", functions.to_json(
        functions.struct($"orderid",$"custid",$"stocksymbol", $"quantity")
      ))

  }

  private def writeToKafka(streamWriter: DataStreamWriter[Row], topic: String, checkpointLocation: String): DataStreamWriter[Row] = {
    streamWriter
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", topic)
      .option("checkpointLocation", checkpointLocation)
  }
}
