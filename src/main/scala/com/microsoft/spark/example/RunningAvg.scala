package com.microsoft.spark.example

import org.apache.spark._
import org.apache.log4j._

object RunningAvg {
  def parseLine(line:String): (Int,Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","RunningAvg")
    val lines = sc.textFile("")
    val rdd = lines.map(parseLine)
  }

}
