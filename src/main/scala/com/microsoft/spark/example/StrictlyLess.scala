package com.microsoft.spark.example
import scala.io.StdIn._
object StrictlyLess {
def main(args:Array[String]): Unit ={
  val inputLine1 =readLine()

  val inputLine1Arr = inputLine1.split(" ").map(_.toInt)
  val inputLine2 = readLine()
  val inputLine2Arr =inputLine2.split(" ").map(_.toInt)
  val numK = inputLine1Arr(1)
  var count = 0
  for(i<-inputLine2Arr){
    if(i<numK){
      count=count+1
    }
  }
  println(count)
}
}
