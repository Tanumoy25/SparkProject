package com.microsoft.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPractice3 extends App  {
  val spark = SparkSession.builder().appName("practice3").master("local[*]").getOrCreate()
  import spark.implicits._

  case class MovieRatings(movieName: String, rating: Double)
  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

  val movies_critics = Seq(
    MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
    MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
  val ratings = movies_critics.toDF

//  val finalDf=ratings.groupBy("name").pivot("movieRatings")
  val finalDf = ratings.selectExpr("name","explode(movieRatings)")

  finalDf.show()
}
