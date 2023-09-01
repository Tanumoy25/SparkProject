package com.microsoft.spark.example
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark._
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.StreamingContext

object WordcountStreaming extends App {
  val sc = new SparkContext("local[*]","wordCount")
  Logger.getLogger("org").setLevel(Level.ERROR)
/*  val ssc= new StreamingContext(sc, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 9998)
  val words = lines.flatMap(x=>x.split(" "))
  val pairs = words.map(x=>(x,1))
  val wordCounts = pairs.reduceByKey(_+_)
  ssc.start()
  ssc.awaitTermination()
*/
}
