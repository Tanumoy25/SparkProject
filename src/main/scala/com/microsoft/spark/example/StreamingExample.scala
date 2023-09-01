package com.microsoft.spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamingExample extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[2")
    .appName("Streaming WordCount")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.shuffle.partitions",3)
    .getOrCreate()

  //.config("spark.sql.streaming.schemaInference","true")

 val ordersDf = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","12345").load()

  val orderSchema = StructType(List(
    StructField("order_id",IntegerType),
    StructField("order_date",StringType),
    StructField("order_customer_id",IntegerType),
    StructField("order_status",StringType),
    StructField("amount",IntegerType)
  ))

/* val valueDf =  ordersDf.select(get_json_object(col("value"),orderSchema)
  .alias("value"))

  val refinedDf = valueDf.select("value.*")
  val windowAggDf = refinedDf
    .groupBy(window(col("order_data"),"15 minute"))
    .agg(sum("amount")
      .alias("totalInvoice"))

  val outputDf = windowAggDf.select("window.start","window.end","totalInvoice")

  outputDf.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation","checkpoint-location1")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start()

 */
}
