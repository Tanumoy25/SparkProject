package com.microsoft.spark.example

import org.apache.spark._
import org.apache.log4j._

object Ratings {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","RatingsCounter")
    val lines = sc.textFile("C:\\Users\\USER\\Desktop\\u.data")
    val ratings = lines.map(x=>x.split("\t")(2))
    val  results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)
  }
}
