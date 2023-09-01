package com.microsoft.spark.example
import scala.io.StdIn._
object RevString {
def main(args:Array[String]): Unit ={
  val input = readLine()
  val output1 = input.reverse
  println(output1)
  val output2= input.split(" ").map(x=>x.reverse)
  println(output2.mkString(" "))
  val output3 = input.split(" ").reverse
  println(output3.mkString(" "))
}
}
