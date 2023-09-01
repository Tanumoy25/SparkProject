package com.microsoft.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{FileNotFoundException, IOException}

object SparkPractice4{
  def main (args:Array[String]):Unit= {
    try {
      val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate
      import spark.implicits._
      val inputDF = Seq(
        ("100", "John", Some(35), None),
        ("100", "John", None, Some("Georgia")),
        ("101", "Mike", Some(25), None),
        ("101", "Mike", None, Some("New York")),
        ("103", "Mary", Some(22), None),
        ("103", "Mary", None, Some("Texas")),
        ("104", "Smith", Some(25), None),
        ("105", "Jake", None, Some("Florida"))).toDF("id", "name", "age", "city")
      val q = inputDF
        .groupBy("id", "name")
        .agg(first("age", ignoreNulls = true) as "age", first("city", ignoreNulls = true) as "city")
        .orderBy("id")
      q.show()
    }

    catch {
      case e: FileNotFoundException => println("Couldn't find file")
      case e: IOException => println("Had IO Exception")
      case ex: Exception => println("Unknown exception")
    }
  }
}
