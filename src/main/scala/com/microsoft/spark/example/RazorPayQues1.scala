package com.microsoft.spark.example

import org.apache.spark.sql.SparkSession

object RazorPayQues1 {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("Razorpay1").master("local[*]").getOrCreate()

  }
}
