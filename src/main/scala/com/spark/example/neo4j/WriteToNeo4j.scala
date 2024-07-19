package com.spark.example.neo4j

import com.spark.example.entities.{Customer, Order, Stock}
import com.spark.example.utils.Util
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession, functions}

object WriteToNeo4j {
  private val APP_NAME = "ReadFromNeo4j"
  private val NEO4J_URL = "neo4j://localhost:7687"
  private val NEO4J_USER = "neo4j"
  private val NEO4J_PWD = "password"

  def main(args: Array[String]): Unit = {
    var neo4jConf: Map[String, String] = Map()
    neo4jConf += ("neo4j.url" -> NEO4J_URL)
    neo4jConf += ("neo4j.authentication.basic.username" -> NEO4J_USER)
    neo4jConf += ("neo4j.authentication.basic.password" -> NEO4J_PWD)
    val spark = Util.GetSparkSession(APP_NAME, neo4jConf)
    spark.sparkContext.setLogLevel(logLevel = "ERROR")

    val stockDs = getStocks(spark)
    writeStockDs(spark, stockDs)

    val customerDs = getCustomers(spark)
    writeCustomer(spark, customerDs)

    val orderDs = getOrders(spark)
    writeOrders(spark, orderDs)

  }

  private def getStocks(spark: SparkSession): Dataset[Stock] = {
    import spark.implicits._
    spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/neo4j/stocks.csv")
      .as[Stock]
  }

  private def writeStockDs(spark: SparkSession, stockDs: Dataset[Stock]): Unit = {
    stockDs
      .coalesce(1)
      .write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", "Stock")
      .option("node.keys", "stocksymbol")
      .save()
  }

  private def getCustomers(spark: SparkSession): Dataset[Customer] = {
    import spark.implicits._
    val customerSchema = Encoders.product[Customer].schema
    spark
      .read
      .option("header", true)
      .schema(customerSchema)
      .csv("data/neo4j/customers.csv")
      .as[Customer]
  }

  private def writeCustomer(spark: SparkSession, stockDs: Dataset[Customer]): Unit = {
    stockDs
      .coalesce(1)
      .write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", "Customer")
      .option("node.keys", "custid")
      .save()
  }

  private def getOrders(spark: SparkSession): Dataset[Order] = {
    import spark.implicits._
    val orderSchema = Encoders.product[Order].schema
    spark
      .read
      .option("header", true)
      .schema(orderSchema)
      .csv("data/neo4j/orders.csv")
      .as[Order]
  }

  private def writeOrders(spark: SparkSession, orders: Dataset[Order]): Unit = {
    orders
      .coalesce(1)
      .write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("relationship", "ORDERED")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "match")
      .option("relationship.source.labels", ":Customer")
      .option("relationship.source.node.keys", "custid")
      .option("relationship.target.save.mode", "match")
      .option("relationship.target.labels", ":Stock")
      .option("relationship.target.node.keys", "stocksymbol")
      .option("relationship.properties", "orderid,quantity")
      .option("relationship.merge", "true")
      .option("relationship.keys", "orderid")
      .save()
  }
}
